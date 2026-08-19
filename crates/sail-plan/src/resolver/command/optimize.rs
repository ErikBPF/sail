use std::collections::HashSet;

use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{LakehouseOperation, TableKind};
use sail_common_datafusion::datasource::{OptimizeInfo, OptionLayer, TableFormatRegistry};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_optimize(
        &self,
        optimize: spec::Optimize,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if optimize.full {
            return Err(PlanError::unsupported(
                "OPTIMIZE FULL is only supported for clustered tables",
            ));
        }

        let (read, path, format, partition_by, properties, lakehouse_table) = match optimize.target
        {
            spec::OptimizeTarget::Path { path } => (
                delta_path_read(&path),
                path,
                "delta".to_string(),
                vec![],
                vec![],
                None,
            ),
            spec::OptimizeTarget::Table { name } if matches!(name.parts(), [format, _] if format.as_ref().eq_ignore_ascii_case("delta")) =>
            {
                let path = name.parts()[1].as_ref().to_string();
                (
                    delta_path_read(&path),
                    path,
                    "delta".to_string(),
                    vec![],
                    vec![],
                    None,
                )
            }
            spec::OptimizeTarget::Table { name } => {
                let table_name: Vec<String> = name.clone().into();
                let status = self
                    .ctx
                    .extension::<CatalogManager>()?
                    .get_table_or_view(name.parts())
                    .await?;
                let TableKind::Table {
                    location,
                    format,
                    partition_by,
                    properties,
                    ..
                } = status.kind
                else {
                    return Err(PlanError::unsupported(
                        "OPTIMIZE is only supported on tables",
                    ));
                };
                let path = location
                    .ok_or_else(|| PlanError::unsupported("OPTIMIZE on tables without location"))?;
                let lakehouse_table = self
                    .resolve_lakehouse_table_context(
                        &table_name,
                        LakehouseOperation::Maintenance,
                        Some(&format),
                        vec![],
                    )
                    .await?;
                let read = named_table_read(name);
                (
                    read,
                    path,
                    format,
                    partition_by,
                    properties,
                    Some(lakehouse_table),
                )
            }
        };

        if !format.eq_ignore_ascii_case("delta") {
            return Err(PlanError::unsupported(format!(
                "OPTIMIZE is not supported for {format} tables"
            )));
        }

        let mut input = self.resolve_query_plan(read, state).await?;
        let condition = if let Some(condition) = optimize.condition {
            let predicate = self
                .resolve_expression(condition.expr, input.schema(), state)
                .await?;
            let external_predicate =
                self.rewrite_expression_for_external_schema(predicate.clone(), state)?;
            let partition_names = partition_by
                .iter()
                .map(|field| field.column.as_str())
                .collect::<HashSet<_>>();
            if !partition_names.is_empty()
                && external_predicate
                    .column_refs()
                    .iter()
                    .any(|column| !partition_names.contains(column.name.as_str()))
            {
                return Err(PlanError::invalid(
                    "OPTIMIZE WHERE predicate can only reference partition columns",
                ));
            }
            input = LogicalPlanBuilder::from(input).filter(predicate)?.build()?;
            Some(ExprWithSource::new(external_predicate, condition.source))
        } else {
            None
        };
        let fields = Self::get_field_names(input.schema(), state)?;
        input = rename_logical_plan(input, &fields)?;

        let z_order_by = optimize
            .z_order_by
            .into_iter()
            .map(|name| {
                let parts: Vec<String> = name.into();
                if parts.len() != 1 {
                    return Err(PlanError::unsupported(
                        "nested Z-Ordering columns are not yet supported",
                    ));
                }
                let column = parts[0].clone();
                input
                    .schema()
                    .field_with_unqualified_name(&column)
                    .map_err(|_| {
                        PlanError::invalid(format!("Z-Ordering column {column:?} does not exist"))
                    })?;
                Ok(column)
            })
            .collect::<PlanResult<Vec<_>>>()?;

        let options = vec![
            OptionLayer::TablePropertyList { items: properties },
            OptionLayer::OptionList {
                items: vec![("path".to_string(), path.clone())],
            },
        ];
        self.ctx
            .extension::<TableFormatRegistry>()?
            .get(&format)?
            .create_optimizer(
                &self.ctx.state(),
                OptimizeInfo {
                    input,
                    path,
                    condition,
                    z_order_by,
                    partition_by,
                    options,
                    lakehouse_table,
                },
            )
            .await
            .map_err(PlanError::from)
    }
}

fn named_table_read(name: spec::ObjectName) -> spec::QueryPlan {
    spec::QueryPlan::new(spec::QueryNode::Read {
        read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
            name,
            temporal: None,
            sample: None,
            options: vec![],
        })),
        is_streaming: false,
    })
}

fn delta_path_read(path: &str) -> spec::QueryPlan {
    spec::QueryPlan::new(spec::QueryNode::Read {
        read_type: spec::ReadType::DataSource(Box::new(spec::ReadDataSource {
            format: Some("delta".to_string()),
            schema: None,
            options: vec![],
            paths: vec![path.to_string()],
            predicates: vec![],
        })),
        is_streaming: false,
    })
}
