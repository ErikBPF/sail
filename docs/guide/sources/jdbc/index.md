---
title: JDBC
rank: 4
---

# JDBC Data Source

Sail provides a database connector exposed under the `jdbc` format name for API parity with vanilla PySpark.
The implementation is based on the Python `connectorx` library.
No actual JDBC driver or JVM is involved.

<!--@include: ../../_common/spark-session.md-->

## Installation

You need to install the `pysail` package with the `jdbc` extra to use the JDBC data source.

```bash
pip install pysail[jdbc]
```

## Quick Start

Register the datasource once per Spark session.

```python
from pysail.spark.datasource.jdbc import JdbcDataSource

spark.dataSource.register(JdbcDataSource)
```

Then read from a database using the standard PySpark API.

```python
df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/mydb")
    .option("dbtable", "public.users")
    .option("user", "alice")
    .option("password", "secret")
    .load()
)
```

Alternatively, you can use the `spark.read.jdbc()` shorthand method.

```python
df = spark.read.jdbc(
    "jdbc:postgresql://localhost:5432/mydb",
    "public.users",
    properties={"user": "alice", "password": "secret"},
)
```

## Options

The data source options are consistent with
the [PySpark JDBC documentation](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html).

| Name                | Required | Default | Description                                                                                                      |
| ------------------- | -------- | ------- | ---------------------------------------------------------------------------------------------------------------- |
| `url`               | Yes      |         | The JDBC URL in the form of `jdbc:<subprotocol>://<host>:<port>/<db>`.                                           |
| `dbtable`           | Yes      |         | The table name, optionally qualified (`<schema>.<table>`). This is mutually exclusive with `query`.              |
| `query`             | Yes      |         | An arbitrary SQL `SELECT` statement. This is mutually exclusive with `dbtable`.                                  |
| `user`              | No       |         | The database username.                                                                                           |
| `password`          | No       |         | The database password.                                                                                           |
| `partitionColumn`   | No       |         | The numeric column for range-stride partitioning. This requires `lowerBound`, `upperBound`, and `numPartitions`. |
| `lowerBound`        | No       |         | The lower bound of partition stride (inclusive).                                                                 |
| `upperBound`        | No       |         | The upper bound of partition stride (inclusive on last partition).                                               |
| `numPartitions`     | No       | `1`     | The number of parallel read partitions.                                                                          |
| `fetchsize`         | No       | `0`     | An advisory rows-per-round-trip hint.                                                                            |
| `pushDownPredicate` | No       | `true`  | Whether to push `WHERE` filters to the database.                                                                 |
| `customSchema`      | No       |         | A Spark DDL string to override inferred column types.                                                            |

::: info
Exactly one of the `dbtable` or `query` options is required.
:::

## Examples

### Custom SQL Queries

Use `query` instead of `dbtable` to run arbitrary SQL queries:

```python
df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/mydb")
    .option("query", "SELECT id, name FROM users WHERE active = TRUE")
    .option("user", "alice")
    .option("password", "secret")
    .load()
)
```

The `query` and `partitionColumn` options are mutually exclusive. To partition a custom query, wrap it in `dbtable` as a
subquery:

```python{4-5}
df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/mydb")
    .option("dbtable", "(SELECT * FROM events WHERE type='click') AS t")
    .option("partitionColumn", "user_id")
    .option("user", "alice")
    .option("password", "secret")
    .load()
)
```

### Parallel Reads with Range Partitioning

Provide `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions` together to split the read into parallel
range strides:

```python
df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/mydb")
    .option("dbtable", "events")
    .option("partitionColumn", "id")
    .option("lowerBound", "1")
    .option("upperBound", "10000000")
    .option("numPartitions", "8")
    .option("user", "alice")
    .option("password", "secret")
    .load()
)
```

Or equivalently, you can use the `spark.read.jdbc()` method with the same options:

```python
df = spark.read.jdbc(
    "jdbc:postgresql://localhost:5432/mydb",
    "events",
    column="id",
    lowerBound=1,
    upperBound=10_000_000,
    numPartitions=8,
    properties={"user": "alice", "password": "secret"},
)
```

### Schema Override

Use `customSchema` to override column types after reading:

```python
df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/mydb")
    .option("dbtable", "orders")
    .option("customSchema", "amount DECIMAL(18,2), status STRING")
    .option("user", "alice")
    .option("password", "secret")
    .load()
)
```

Columns not listed in `customSchema` retain their inferred types.

## Writing

Writes use the same `jdbc` format name and the same `pysail[jdbc]` extra.

PostgreSQL uses Arrow-native ADBC bulk ingest. MySQL and SQL Server use a
SQLAlchemy-core fallback (a parameterised `INSERT` built directly from Arrow
values) and need their DBAPI driver installed separately: `pip install pymysql`
(MySQL) or `pip install pymssql` (SQL Server). The Arrow-value path preserves
exact integers (including bigints above 2^53) and keeps `NULL` distinct from a
numeric zero.

For SQL Server, extra JDBC URL parameters (`;encrypt=...;trustServerCertificate=...;applicationIntent=...`)
are parsed and mapped onto the pymssql connection: `encrypt` sets pymssql's
`encryption` mode, `applicationIntent=ReadOnly` sets a read-only connection, and
`trustServerCertificate` is accepted but ignored (FreeTDS, pymssql's backend,
governs certificate trust through its own configuration). Unknown parameters are
ignored.

```python
(
    df.write.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/mydb")
    .option("dbtable", "orders")
    .option("user", "alice")
    .option("password", "secret")
    .mode("append")  # or "overwrite"
    .save()
)
```

### Save modes

- **append** — direct ingest into the target. At-least-once: a retried/speculated
  Spark task re-ingests its rows (matches Spark's built-in JDBC writer, which
  makes no exactly-once guarantee), so the target may gain duplicates.
- **overwrite** — by default, drops and recreates the target from the DataFrame
  schema before writing, matching Spark's built-in JDBC writer. For MySQL and
  SQL Server, `truncate=true` truncates instead, preserving the table schema and
  metadata. Spark's PostgreSQL dialect does not support this optimization, so
  PostgreSQL ignores `truncate=true` and still drops/recreates.
  - PostgreSQL also supports the Sail extension `overwriteMode=atomic`, which loads all
    partitions into one staging table, then swaps it over the target in a single
    `DROP; RENAME` transaction (replaces the table object, so grants/ACLs/RLS are
    **not** preserved); the target is never left partially written. It is, however,
    **at-least-once** under Spark task retry / speculative execution — a re-run
    partition re-ingests into the shared staging, so the swapped-in table can contain
    duplicates (same as `append`); disable speculation for exactly-once overwrite.
    `overwriteMode=truncate`
    `TRUNCATE`s the target once then ingests directly, preserving the table object
    but **non-atomically**. These extension modes intentionally differ from Spark
    and must be requested explicitly.

::: warning
Concurrent overwrites to the same table are **not supported**. Two overwrite jobs
running at once can interleave and leave a mixed result; Spark's own JDBC writer
takes no lock and gives the same non-guarantee. Run overwrites one at a time.

A failed cleanup can leave orphan `*__sail_stg_*` (and, for PostgreSQL truncate
mode, `*__sail_trunc_*`) tables behind. They are safe to drop manually.
:::

| Write option    | Default  | Description                                        |
| --------------- | -------- | -------------------------------------------------- |
| `dbtable`       |          | Target table (required; `query` not allowed)       |
| `truncate`      | `false`  | Preserve table on MySQL/SQL Server overwrite      |
| `overwriteMode` | `spark`  | `spark`, `atomic`, or `truncate` (PostgreSQL only) |
| `batchsize`     | `65536`  | Rows per ingest call                               |

In explicit `append` or `overwrite` mode, a missing target is created from the
DataFrame schema. PySpark's Python data-source API currently rejects the default
`errorIfExists` mode before the JDBC writer runs, so specify a mode explicitly.
