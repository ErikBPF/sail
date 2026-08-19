# Sail 0.7.0 `OPTIMIZE` plan

## Compatibility target

`OPTIMIZE` is a Delta Lake Spark extension, not an Apache Spark core SQL command. Target the open-source Delta Lake contract:

```sql
OPTIMIZE (string_path | qualified_table) [FULL]
  [WHERE partition_predicate]
  [ZORDER BY [(] qualified_column [, ...] [)]]
```

- Plain `OPTIMIZE`: bin-pack small files independently per partition.
- `WHERE`: accept boolean predicates using partition columns only.
- `ZORDER BY`: accept nested columns, with or without parentheses; reject missing columns, partition columns, and columns without collected statistics.
- `FULL`: parse it now, but reject it until Sail supports Delta liquid clustering with non-empty clustering columns. It is not generic force-compaction.
- Named and path-based tables: support catalog-qualified names, Delta backtick paths, and quoted string paths.

References:

- https://docs.delta.io/optimizations-oss/
- https://docs.delta.io/delta-clustering/
- https://github.com/delta-io/delta/blob/master/spark/src/main/antlr4/io/delta/sql/parser/DeltaSqlBase.g4
- https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/commands/OptimizeTableCommand.scala

## Plan review and local scope

Approved with these corrections and explicit limits:

- Tests count active files by replaying Delta `add`/`remove` actions. Physical Parquet file counts are invalid because optimization tombstones old files instead of deleting them immediately.
- The local implementation rewrites selected files through Sail's existing Delta writer and target-file-size splitting. Size-aware bin selection remains required before claiming bin-packing compatibility.
- Local `ZORDER BY` uses lexicographic sorting. Morton keys, statistics eligibility checks, and Delta Z-cube tags remain required before claiming Z-order compatibility.
- `FULL` remains an explicit error until liquid clustering exists.
- Commit history includes `OPTIMIZE`, parameters, and existing file metrics. The Delta-compatible SQL `path`/`metrics` result row remains follow-up work.

## Observable contract

- Query results remain identical before and after optimization.
- New `add` and matching `remove` actions use `dataChange=false`; no Change Data Feed rows are produced.
- Unselected partitions remain untouched.
- A second plain compaction with no new eligible files is a no-op.
- Readers continue using their prior snapshot while the rewrite commits atomically.
- Commit history records operation `OPTIMIZE`, predicate/Z-order parameters, and file/byte metrics.
- SQL result exposes Delta-compatible `path` and `metrics` columns. Start with core file metrics; add the full nested metrics schema before compatibility is declared complete.
- Deletion-vector files are materialized during rewrite and reported as removed.
- Column mapping, row tracking, catalog-managed commits, and transaction retries use existing Delta read/write paths rather than parallel implementations.
- Z-order output files carry Delta Z-cube tags; ordinary compaction preserves only tags that remain valid after rewrite.

## Minimal implementation path

1. Parser and spec
   - Add `OPTIMIZE` and `ZORDER` keywords; `FULL`, `WHERE`, and `BY` already exist.
   - Add one statement AST variant with target, `full`, predicate, and Z-order columns.
   - Represent target as an enum (`Table` or `Path`) so invalid mixed states cannot exist.
   - Add matching `CommandNode::Optimize` fields and preserve original predicate text for Delta history.

2. Analyzer and resolver
   - Convert AST expressions through existing SQL expression conversion.
   - Resolve named tables through `CatalogManager`; resolve paths through Delta source options.
   - Require Delta format, a mutable table location, and `LakehouseOperation::Maintenance` authority.
   - Resolve the predicate against table schema, then enforce partition-only references.
   - Resolve Z-order paths against logical/physical column mapping and statistics availability.
   - Return the canonical `FULL` unsupported error until liquid clustering exists.

3. Reuse table-format plumbing
   - Add `OptimizeInfo` plus a default-unsupported `TableFormat::create_optimizer`, matching existing delete/merge hooks.
   - Delta implementation returns one `DeltaOptimizeNode`; other formats need no code.
   - Extend `DeltaPhysicalPlanner` for that node.

4. Compaction physical plan
   - Reuse log replay, metadata filtering, `DeltaDiscoveryExec`, `DeltaScanByAddsExec`, writer, and commit assembly.
   - Group active `Add` actions by partition values.
   - Sort by size and greedily bin to existing `target_file_size`; schedule only bins containing at least two files or a deletion vector.
   - Scan each selected bin once, write replacement Parquet files, and remove exactly those input actions.
   - Add `DeltaOperation::Optimize`; treat it as no data change for conflict checks and emit core operation metrics.
   - Do not add another compaction framework or dependency.

5. Z-order follow-up
   - Reuse the same discovery/write/commit path.
   - Rewrite all selected files per partition, range-normalize supported scalar columns, interleave bits into a Morton key, repartition/sort by that key, then drop the key before write.
   - Tag outputs with Z-cube ID, columns, and curve metadata.
   - Add type/null/nested-column tests before enabling this path.

6. Liquid clustering follow-up
   - Implement clustering table protocol/domain metadata first.
   - Then enable `FULL`, Hilbert/Z-cube planning, incremental clustering, and full reclustering.

## Test order

1. Parser RED: all documented target and clause forms, including optional Z-order parentheses.
2. Compaction RED/GREEN: three small files become one; rows unchanged; add/remove actions have `dataChange=false`; history metrics present.
3. Idempotence: repeated plain `OPTIMIZE` keeps one active file and performs no rewrite.
4. Partition predicate: selected partition compacts; other partition file count stays unchanged; non-partition predicate fails.
5. Z-order: multi-column syntax, row preservation, Z-cube tags, invalid/missing/partition columns.
6. `FULL`: non-clustered table returns explicit unsupported error; clustered behavior remains pending clustering support.
7. Edge cases: empty table, one file, null partition values, DVs, column mapping, concurrent append, catalog-managed commit, path targets, and output metrics schema.

Implemented local contracts:

- `crates/sail-sql-analyzer/src/parser.rs::test_parse_optimize`
- `python/pysail/tests/spark/delta/features/optimize.feature`
