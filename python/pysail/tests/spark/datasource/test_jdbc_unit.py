"""Pure-unit tests for the JDBC data source — no Spark session or DB container required.

No ``pytestmark = pytest.mark.integration``, so these run without Docker on every
Spark tier that has the Python DataSource API. They import ``jdbc.py``, which needs
``pyspark.sql.datasource`` (Spark 4.0+), so the module is skipped below that. Tests
exercising filter pushdown additionally need the 4.1-only pushdown classes and are
individually marked below.
"""

from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pytest

from pysail.testing.spark.utils.common import pyspark_version

try:
    from pyspark.sql.datasource import DataSourceArrowWriter  # noqa: F401  (Spark 4.0+)
except ImportError:
    pytest.skip("JDBC data source requires the PySpark Python DataSource API (4.0+)", allow_module_level=True)


@pytest.fixture
def stub_target_exists(monkeypatch):
    """Stub out the driver-side target-existence check for tests that exercise writer
    dispatch/option resolution rather than connectivity."""
    from pysail.spark.datasource import jdbc as jdbc_mod

    monkeypatch.setattr(jdbc_mod, "_pg_table_exists", lambda *_a, **_k: True)
    monkeypatch.setattr(jdbc_mod, "_sqlalchemy_table_exists", lambda *_a, **_k: True)
    monkeypatch.setattr(jdbc_mod, "_drop_pg_table", lambda *_a, **_k: None)
    monkeypatch.setattr(jdbc_mod, "_reset_sqlalchemy_table", lambda *_a, **_k: None)
    monkeypatch.setattr(jdbc_mod, "_create_pg_table", lambda *_a, **_k: None)
    monkeypatch.setattr(jdbc_mod, "_create_sqlalchemy_table", lambda *_a, **_k: None)
    monkeypatch.setattr(jdbc_mod, "_truncate_pg_table", lambda *_a, **_k: None)
    monkeypatch.setattr(jdbc_mod, "_pg_target_columns", lambda *_a, **_k: ["id"])
    monkeypatch.setattr(jdbc_mod, "_sqlalchemy_target_columns", lambda *_a, **_k: ["id"])


# ---------------------------------------------------------------------------
# _filter_to_sql
# ---------------------------------------------------------------------------


@pytest.mark.skipif(pyspark_version() < (4, 1), reason="filter pushdown requires PySpark 4.1+")
def test_filter_to_sql_unit():
    from pyspark.sql.datasource import (
        EqualTo,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
    )

    from pysail.spark.datasource.jdbc import _filter_to_sql

    cases = [
        (EqualTo(("age",), 28), '"age" = 28'),
        (GreaterThan(("score",), 9.0), '"score" > 9.0'),
        (GreaterThanOrEqual(("id",), 1), '"id" >= 1'),
        (LessThan(("age",), 40), '"age" < 40'),
        (LessThanOrEqual(("age",), 35), '"age" <= 35'),
        (EqualTo(("name",), "Alice"), "\"name\" = 'Alice'"),
        (EqualTo(("name",), "O'Reilly"), "\"name\" = 'O''Reilly'"),
        (EqualTo(("active",), True), '"active" = TRUE'),
        (EqualTo(("active",), False), '"active" = FALSE'),
    ]

    for f, expected in cases:
        result = _filter_to_sql(f)
        assert result == expected, f"_filter_to_sql({f!r}) = {result!r}, expected {expected!r}"


# ---------------------------------------------------------------------------
# _jdbc_url_to_dsn
# ---------------------------------------------------------------------------


def test_jdbc_url_to_dsn_unit():
    from pysail.spark.datasource.jdbc import _jdbc_url_to_dsn

    assert _jdbc_url_to_dsn("jdbc:postgresql://localhost:5432/db", None, None) == "postgresql://localhost:5432/db"
    assert (
        _jdbc_url_to_dsn("jdbc:postgresql://localhost:5432/db", "alice", None) == "postgresql://alice@localhost:5432/db"
    )
    assert (
        _jdbc_url_to_dsn("jdbc:postgresql://localhost:5432/db", "alice", "secret")
        == "postgresql://alice:secret@localhost:5432/db"
    )

    result = _jdbc_url_to_dsn("jdbc:postgresql://h:5432/db", "u", "p@ss/w0rd")
    assert "p%40ss%2Fw0rd" in result

    with pytest.raises(ValueError, match="Invalid JDBC URL"):
        _jdbc_url_to_dsn("postgresql://localhost/db", None, None)


def test_error_messages_redact_credentials():
    """Credentials embedded in a URL must not leak into error messages."""
    from pysail.spark.datasource.jdbc import _jdbc_url_to_dsn, _redact_credentials

    assert _redact_credentials("postgresql://user:s3cret@host/db") == "postgresql://<redacted>@host/db"
    assert _redact_credentials("jdbc:postgresql://host/db") == "jdbc:postgresql://host/db"

    # A malformed URL with embedded credentials must be scrubbed in the raised message.
    with pytest.raises(ValueError, match="Invalid JDBC URL") as exc:
        _jdbc_url_to_dsn("postgresql://alice:topsecret@host/db", None, None)
    assert "topsecret" not in str(exc.value)
    assert "<redacted>" in str(exc.value)


# ---------------------------------------------------------------------------
# _parse_custom_schema
# ---------------------------------------------------------------------------


def test_parse_custom_schema_unit():
    from pysail.spark.datasource.jdbc import _parse_custom_schema

    result = _parse_custom_schema("id BIGINT, name STRING, score DOUBLE, active BOOLEAN")
    assert result["id"] == pa.int64()
    assert result["name"] == pa.large_utf8()
    assert result["score"] == pa.float64()
    assert result["active"] == pa.bool_()

    result = _parse_custom_schema("price DECIMAL(10,2)")
    assert result["price"] == pa.decimal128(10, 2)

    result = _parse_custom_schema("MyCol INTEGER")
    assert "mycol" in result
    assert result["mycol"] == pa.int32()

    assert _parse_custom_schema("") == {}


# ---------------------------------------------------------------------------
# _lit (via _filter_to_sql)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(pyspark_version() < (4, 1), reason="filter pushdown requires PySpark 4.1+")
def test_lit_unit():
    from pyspark.sql.datasource import EqualTo

    from pysail.spark.datasource.jdbc import _filter_to_sql

    def lit(v):
        return _filter_to_sql(EqualTo(("x",), v)).split(" = ", 1)[1]

    assert lit(True) == "TRUE"
    assert lit(False) == "FALSE"
    assert lit(None) == "NULL"
    assert lit(42) == "42"
    assert lit(3.14) == "3.14"
    assert lit("hello") == "'hello'"
    assert lit("O'Reilly") == "'O''Reilly'"
    assert lit(dt.date(2024, 1, 15)) == "'2024-01-15'"
    assert lit(dt.datetime(2024, 1, 15, 10, 30)) == "'2024-01-15T10:30:00'"  # noqa: DTZ001


# ---------------------------------------------------------------------------
# _quote_identifier
# ---------------------------------------------------------------------------


def test_quote_identifier_unit():
    from pysail.spark.datasource.jdbc import _quote_identifier

    assert _quote_identifier("age") == '"age"'
    assert _quote_identifier("my col") == '"my col"'
    assert _quote_identifier('col"name') == '"col""name"'
    assert _quote_identifier("") == '""'


# ---------------------------------------------------------------------------
# writer() dialect dispatch
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("stub_target_exists")
def test_write_supported_dialects_return_writers():
    """PostgreSQL, MySQL and SQL Server URLs return a writer (dispatch, not connectivity)."""
    from pyspark.sql.types import IntegerType, StructField, StructType

    from pysail.spark.datasource.jdbc import (
        JdbcDataSource,
        JdbcDataSourceWriter,
        SqlAlchemyDataSourceWriter,
    )

    schema = StructType([StructField("id", IntegerType())])
    cases = [
        ("jdbc:postgresql://localhost:5432/db", JdbcDataSourceWriter),
        ("jdbc:mysql://localhost:3306/db", SqlAlchemyDataSourceWriter),
        ("jdbc:sqlserver://localhost:1433;databaseName=db", SqlAlchemyDataSourceWriter),
    ]
    for url, expected in cases:
        ds = JdbcDataSource(options={"url": url, "dbtable": "t"})
        assert isinstance(ds.writer(schema, overwrite=False), expected)


def test_sqlalchemy_table_exists_detects_missing(tmp_path):
    """_sqlalchemy_table_exists reflects the live database, not the DataFrame."""
    sa = pytest.importorskip("sqlalchemy")

    from pysail.spark.datasource.jdbc import _sqlalchemy_table_exists

    url = f"sqlite:///{tmp_path / 'x.db'}"
    setup = sa.create_engine(url)
    with setup.begin() as conn:
        conn.execute(sa.text("CREATE TABLE present (id INTEGER)"))
    setup.dispose()

    assert _sqlalchemy_table_exists(url, {}, "present") is True
    assert _sqlalchemy_table_exists(url, {}, "absent") is False


def test_sqlalchemy_create_missing_table_from_arrow(tmp_path):
    """Missing targets are created once from the incoming Arrow schema."""
    sa = pytest.importorskip("sqlalchemy")
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import _create_sqlalchemy_table

    url = f"sqlite:///{tmp_path / 'create.db'}"
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("name", pa.string())])
    _create_sqlalchemy_table(url, {}, "created", schema)

    engine = sa.create_engine(url)
    try:
        columns = {c["name"]: c for c in sa.inspect(engine).get_columns("created")}
    finally:
        engine.dispose()
    assert set(columns) == {"id", "name"}
    assert columns["id"]["nullable"] is False


def test_arrow_to_sqlalchemy_type_mapping():
    """Auto-create covers scalar Arrow types without integer precision loss."""
    sa = pytest.importorskip("sqlalchemy")
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import _arrow_to_sqlalchemy_type

    assert isinstance(_arrow_to_sqlalchemy_type(pa.bool_()), sa.Boolean)
    assert isinstance(_arrow_to_sqlalchemy_type(pa.int32()), sa.Integer)
    assert isinstance(_arrow_to_sqlalchemy_type(pa.int64()), sa.BigInteger)
    assert isinstance(_arrow_to_sqlalchemy_type(pa.uint64()), sa.Numeric)
    assert isinstance(_arrow_to_sqlalchemy_type(pa.float64()), sa.Float)
    assert isinstance(_arrow_to_sqlalchemy_type(pa.string()), sa.Text)
    assert isinstance(_arrow_to_sqlalchemy_type(pa.binary()), sa.LargeBinary)
    assert isinstance(_arrow_to_sqlalchemy_type(pa.date32()), sa.Date)
    assert isinstance(_arrow_to_sqlalchemy_type(pa.time64("us")), sa.Time)
    assert isinstance(_arrow_to_sqlalchemy_type(pa.timestamp("us")), sa.DateTime)

    decimal = _arrow_to_sqlalchemy_type(pa.decimal128(18, 3))
    assert isinstance(decimal, sa.Numeric)
    assert (decimal.precision, decimal.scale) == (18, 3)

    assert str(_arrow_to_sqlalchemy_type(pa.string(), "mysql")) == "LONGTEXT"
    assert str(_arrow_to_sqlalchemy_type(pa.binary(), "mysql")) == "BLOB"

    from sqlalchemy.dialects import mssql

    assert _arrow_to_sqlalchemy_type(pa.string(), "mssql").compile(dialect=mssql.dialect()) == "NVARCHAR(max)"
    assert _arrow_to_sqlalchemy_type(pa.binary(), "mssql").compile(dialect=mssql.dialect()) == "VARBINARY(max)"
    assert _arrow_to_sqlalchemy_type(pa.timestamp("us"), "mssql").compile(dialect=mssql.dialect()) == "DATETIME"

    with pytest.raises(TypeError, match="does not support"):
        _arrow_to_sqlalchemy_type(pa.list_(pa.int32()), "mysql")
    with pytest.raises(TypeError, match="does not support"):
        _arrow_to_sqlalchemy_type(pa.struct([("x", pa.int32())]), "mssql")


@pytest.mark.parametrize("bad", ["0", "-1", "not-an-integer"])
def test_write_batchsize_must_be_positive(bad):
    """writer() rejects a non-positive batchsize on the driver, before fan-out.

    A zero batchsize would raise at executor runtime (``range`` step 0), and a negative
    one would silently skip all inserts while still reporting rows written.
    """
    from pyspark.sql.types import IntegerType, StructField, StructType

    from pysail.spark.datasource.jdbc import JdbcDataSource

    schema = StructType([StructField("id", IntegerType())])
    ds = JdbcDataSource(options={"url": "jdbc:postgresql://localhost:5432/db", "dbtable": "t", "batchsize": bad})
    with pytest.raises(ValueError, match="batchsize"):
        ds.writer(schema, overwrite=False)


@pytest.mark.parametrize("bad", ["0", "-1", "not-an-integer"])
def test_write_num_partitions_must_be_positive_integer(bad):
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import JdbcDataSource

    ds = JdbcDataSource(
        options={
            "url": "jdbc:postgresql://localhost/db",
            "dbtable": "t",
            "numPartitions": bad,
        }
    )
    with pytest.raises(ValueError, match="numPartitions"):
        ds.writer(pa.schema([("id", pa.int32())]), overwrite=False)


def test_arrow_chunks_honor_batchsize_and_partial_tail():
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import _iter_arrow_chunks

    table = pa.table({"id": range(5)})
    chunks = list(_iter_arrow_chunks(table, 2))

    assert [chunk.num_rows for chunk in chunks] == [2, 2, 1]
    assert [value.as_py() for chunk in chunks for value in chunk["id"]] == list(range(5))


def test_sqlalchemy_insert_calls_respect_batchsize(tmp_path):
    sa = pytest.importorskip("sqlalchemy")
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import SqlAlchemyWriteEngine

    url = f"sqlite:///{tmp_path / 'target.db'}"
    engine = sa.create_engine(url)
    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE t (id INTEGER)"))
    table = sa.Table("t", sa.MetaData(), autoload_with=engine)
    batch_lengths = []

    @sa.event.listens_for(engine, "before_cursor_execute")
    def record_batch(_conn, _cursor, statement, parameters, _context, executemany):
        if statement.lstrip().upper().startswith("INSERT"):
            batch_lengths.append(len(parameters) if executemany else 1)

    writer = SqlAlchemyWriteEngine(
        url=url,
        dbtable="t",
        columns=["id"],
        overwrite=False,
        batch_size=2,
        run_id="run",
    )
    try:
        writer._insert_arrow(engine, table, pa.table({"id": range(5)}))  # noqa: SLF001
    finally:
        engine.dispose()

    assert batch_lengths == [2, 2, 1]


def test_postgres_adbc_ingest_calls_respect_batchsize(monkeypatch):
    import adbc_driver_postgresql.dbapi as pg_dbapi
    import pyarrow as pa

    from pysail.spark.datasource import jdbc

    ingested = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def adbc_ingest(self, _table, chunk, **_kwargs):
            ingested.append(chunk.num_rows)

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

    monkeypatch.setattr(pg_dbapi, "connect", lambda *_a, **_k: FakeConnection())
    monkeypatch.setattr(jdbc, "_pg_target_columns", lambda *_a, **_k: ["id"])
    writer = jdbc.PgWriteEngine(
        dsn="postgresql://unused",
        dbtable="t",
        batch_size=2,
    )
    writer.write_partition(0, [pa.RecordBatch.from_pydict({"id": range(5)})])

    assert ingested == [2, 2, 1]


def test_sqlalchemy_writer_streams_before_requesting_next_batch(tmp_path):
    sa = pytest.importorskip("sqlalchemy")
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import SqlAlchemyWriteEngine

    url = f"sqlite:///{tmp_path / 'target.db'}"
    engine = sa.create_engine(url)
    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE t (id INTEGER)"))
    inserts = []

    @sa.event.listens_for(sa.engine.Engine, "before_cursor_execute")
    def record_insert(_conn, _cursor, statement, parameters, _context, _executemany):
        if statement.lstrip().upper().startswith("INSERT"):
            inserts.append(parameters)

    writer = SqlAlchemyWriteEngine(
        url=url,
        dbtable="t",
        columns=["id"],
        overwrite=False,
        batch_size=1000,
        run_id="stream",
    )

    def batches():
        yield pa.RecordBatch.from_pydict({"id": [1]})
        assert len(inserts) == 1
        yield pa.RecordBatch.from_pydict({"id": [2]})

    try:
        writer.write_partition(0, batches())
        with engine.connect() as conn:
            assert conn.execute(sa.text("SELECT id FROM t ORDER BY id")).fetchall() == [(1,), (2,)]
    finally:
        sa.event.remove(sa.engine.Engine, "before_cursor_execute", record_insert)
        engine.dispose()


def test_postgres_writer_streams_before_requesting_next_batch(monkeypatch):
    import adbc_driver_postgresql.dbapi as pg_dbapi
    import pyarrow as pa

    from pysail.spark.datasource import jdbc

    ingested = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def adbc_ingest(self, _table, chunk, **_kwargs):
            ingested.extend(chunk["id"].to_pylist())

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

    def batches():
        yield pa.RecordBatch.from_pydict({"id": [1]})
        assert ingested == [1]
        yield pa.RecordBatch.from_pydict({"id": [2]})

    monkeypatch.setattr(pg_dbapi, "connect", lambda *_a, **_k: FakeConnection())
    monkeypatch.setattr(jdbc, "_pg_target_columns", lambda *_a, **_k: ["id"])
    writer = jdbc.PgWriteEngine(dsn="postgresql://unused", dbtable="t")
    writer.write_partition(0, batches())

    assert ingested == [1, 2]


def test_sqlalchemy_staging_create_is_retry_safe(tmp_path):
    """Re-creating a staging table of the same name must drop-then-create, not raise
    "table already exists" (defends against a leftover from a crashed prior attempt).
    """
    sa = pytest.importorskip("sqlalchemy")

    from pysail.spark.datasource.jdbc import SqlAlchemyWriteEngine

    url = f"sqlite:///{tmp_path / 'target.db'}"
    setup = sa.create_engine(url)
    with setup.begin() as conn:
        conn.execute(sa.text("CREATE TABLE t (id INTEGER)"))
    setup.dispose()

    writer = SqlAlchemyWriteEngine(url=url, dbtable="t", columns=["id"], overwrite=True, batch_size=1000, run_id="run")
    engine = writer._create_engine()  # noqa: SLF001
    try:
        staging = writer._staging("tok")  # noqa: SLF001
        writer._create_staging_like_target(engine, staging)  # noqa: SLF001
        writer._create_staging_like_target(engine, staging)  # second create must not raise  # noqa: SLF001
    finally:
        engine.dispose()


def test_sqlalchemy_partition_failure_removes_created_staging(tmp_path, monkeypatch):
    """Failure after CREATE must not leave a random staging table unknown to abort()."""
    sa = pytest.importorskip("sqlalchemy")
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import SqlAlchemyWriteEngine

    url = f"sqlite:///{tmp_path / 'target.db'}"
    setup = sa.create_engine(url)
    with setup.begin() as conn:
        conn.execute(sa.text("CREATE TABLE t (id INTEGER)"))
    setup.dispose()

    writer = SqlAlchemyWriteEngine(
        url=url,
        dbtable="t",
        columns=["id"],
        overwrite=True,
        batch_size=1000,
        run_id="run",
    )

    def fail_after_create(*_args, **_kwargs):
        raise RuntimeError("injected ingest failure")

    monkeypatch.setattr(writer, "_insert_arrow", fail_after_create)
    with pytest.raises(RuntimeError, match="injected ingest failure"):
        writer.write_partition(0, [pa.RecordBatch.from_pydict({"id": [1]})])

    check = sa.create_engine(url)
    try:
        assert sa.inspect(check).get_table_names() == ["t"]
    finally:
        check.dispose()


def test_sqlalchemy_overwrite_distinct_partitions_no_data_loss(tmp_path):
    """Two partitions in one overwrite must not clobber each other's staging.

    Sail leaves partitionId() at 0 for every partition, so a partition-id-derived staging
    name would collide and one partition would drop another's rows mid-write. Names are
    uniquified per write_partition call instead; committing both must keep every row.
    """
    sa = pytest.importorskip("sqlalchemy")
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import SqlAlchemyWriteEngine

    url = f"sqlite:///{tmp_path / 'target.db'}"
    setup = sa.create_engine(url)
    with setup.begin() as conn:
        conn.execute(sa.text("CREATE TABLE t (id INTEGER, name TEXT)"))
        conn.execute(sa.text("INSERT INTO t VALUES (99, 'old')"))
    setup.dispose()

    writer = SqlAlchemyWriteEngine(
        url=url, dbtable="t", columns=["id", "name"], overwrite=True, batch_size=1000, run_id="run"
    )
    b1 = pa.RecordBatch.from_pydict({"id": [1, 2], "name": ["a", "b"]})
    b2 = pa.RecordBatch.from_pydict({"id": [3, 4], "name": ["c", "d"]})
    # Both partitions report id 0 (as Sail does); the staging names must still differ.
    r1 = writer.write_partition(0, [b1])
    r2 = writer.write_partition(0, [b2])
    assert r1.staging_table != r2.staging_table

    assert writer.commit([r1, r2]) == 4  # noqa: PLR2004

    check = sa.create_engine(url)
    with check.begin() as conn:
        rows = sorted(conn.execute(sa.text("SELECT id, name FROM t")).fetchall())
    check.dispose()
    assert rows == [(1, "a"), (2, "b"), (3, "c"), (4, "d")]  # old row gone, both partitions kept


def test_write_engines_reject_nonpositive_batch_size():
    """Both engines validate batch_size on construction (defence in depth)."""
    from pysail.spark.datasource.jdbc import PgWriteEngine, SqlAlchemyWriteEngine

    with pytest.raises(ValueError, match="batch_size"):
        PgWriteEngine(dsn="postgresql://x/y", dbtable="t", batch_size=0)
    with pytest.raises(ValueError, match="batch_size"):
        SqlAlchemyWriteEngine(url="sqlite://", dbtable="t", columns=["id"], overwrite=True, batch_size=0, run_id="r")


def test_write_unsupported_dialect_raises():
    """Dialects without a write backend raise a clear error."""
    from pyspark.sql.types import IntegerType, StructField, StructType

    from pysail.spark.datasource.jdbc import JdbcDataSource

    schema = StructType([StructField("id", IntegerType())])
    for url in ["jdbc:oracle:thin:@//localhost:1521/db", "jdbc:h2:mem:test"]:
        ds = JdbcDataSource(options={"url": url, "dbtable": "t"})
        with pytest.raises(ValueError, match="PostgreSQL, MySQL and SQL Server"):
            ds.writer(schema, overwrite=False)


def test_sqlalchemy_url_translation():
    """_sqlalchemy_url maps stripped JDBC DSNs to (SQLAlchemy URL, connect_args)."""
    from pysail.spark.datasource.jdbc import _sqlalchemy_url

    assert _sqlalchemy_url("mysql://u:p@h:3306/db") == ("mysql+pymysql://u:p@h:3306/db", {})
    # host-only forms (no credentials, no explicit port) must parse too
    assert _sqlalchemy_url("mysql://h/db") == ("mysql+pymysql://h/db", {})
    assert _sqlalchemy_url("mysql://h:3306/db") == ("mysql+pymysql://h:3306/db", {})
    url, connect_args = _sqlalchemy_url("sqlserver://u:p@h:1433;databaseName=db")
    assert url == "mssql+pymssql://u:p@h:1433/db"
    assert connect_args == {}
    with pytest.raises(ValueError, match="Unsupported"):
        _sqlalchemy_url("oracle://u:p@h:1521/db")


def test_sqlserver_url_parsing_preserves_params():
    """Supported SQL Server properties are preserved without weakening TLS."""
    from pysail.spark.datasource.jdbc import _parse_sqlserver_url, _sqlalchemy_url

    url, connect_args = _sqlalchemy_url(
        "sqlserver://u:p@h:1433;databaseName=db;encrypt=true;applicationIntent=ReadOnly"
    )
    assert url == "mssql+pymssql://u:p@h:1433/db"
    assert connect_args["encryption"] == "require"
    assert connect_args["read_only"] is True
    # encrypt=false maps to encryption off
    _, ca = _parse_sqlserver_url("u:p@h:1433;databaseName=db;encrypt=false")
    assert ca["encryption"] == "off"

    # No credentials, named instance, no explicit port
    url2, _ = _parse_sqlserver_url("host\\SQLEXPRESS;databaseName=db")
    assert url2 == "mssql+pymssql://host\\SQLEXPRESS/db"

    # Host + port, no database
    url3, ca3 = _parse_sqlserver_url("h:1433")
    assert url3 == "mssql+pymssql://h:1433"
    assert ca3 == {}

    # Instance + port together
    url4, _ = _parse_sqlserver_url("h\\inst:1433;databaseName=db")
    assert url4 == "mssql+pymssql://h\\inst:1433/db"

    # Unknown params are ignored, not carried
    _, ca5 = _parse_sqlserver_url("h:1433;databaseName=db;someFutureParam=x")
    assert "somefutureparam" not in ca5
    assert ca5 == {}


def test_sqlserver_url_credentials_as_params():
    """MS JDBC commonly carries credentials as ;user=;password= params, not in the
    authority. Those must land in the SQLAlchemy URL userinfo, not be dropped.
    """
    from pysail.spark.datasource.jdbc import _parse_sqlserver_url

    # The canonical MS form: creds as semicolon params
    url, _ = _parse_sqlserver_url("h;databaseName=db;user=alice;password=s3cret")
    assert url == "mssql+pymssql://alice:s3cret@h/db"

    # `username` is accepted as an alias for `user`
    url2, _ = _parse_sqlserver_url("h:1433;databaseName=db;username=bob;password=pw")
    assert url2 == "mssql+pymssql://bob:pw@h:1433/db"

    # Special characters in the password are percent-encoded so the URL stays parseable
    url3, _ = _parse_sqlserver_url("h;databaseName=db;user=alice;password=p@ss/w:rd")
    assert url3 == "mssql+pymssql://alice:p%40ss%2Fw%3Ard@h/db"

    # User without a password
    url4, _ = _parse_sqlserver_url("h;databaseName=db;user=alice")
    assert url4 == "mssql+pymssql://alice@h/db"

    # Credentials already in the authority win; params do not override them
    url5, _ = _parse_sqlserver_url("u:p@h:1433;databaseName=db;user=alice;password=s3cret")
    assert url5 == "mssql+pymssql://u:p@h:1433/db"


@pytest.mark.parametrize("value", ["strict", "mandatory", "optional", "garbage", ""])
def test_sqlserver_url_rejects_encrypt_values_pymssql_cannot_preserve(value):
    """Never weaken JDBC TLS semantics by silently translating them to encryption=off."""
    from pysail.spark.datasource.jdbc import _parse_sqlserver_url

    with pytest.raises(ValueError, match="encrypt"):
        _parse_sqlserver_url(f"h;databaseName=db;encrypt={value}")


@pytest.mark.parametrize(
    "property_name",
    ["trustServerCertificate", "hostNameInCertificate", "trustStore", "trustStorePassword"],
)
def test_sqlserver_url_rejects_unhonored_jdbc_tls_properties(property_name):
    from pysail.spark.datasource.jdbc import _parse_sqlserver_url

    with pytest.raises(ValueError, match=property_name):
        _parse_sqlserver_url(f"h;databaseName=db;{property_name}=value")


def test_sqlserver_tls_rejection_redacts_parameter_password():
    from pysail.spark.datasource.jdbc import _parse_sqlserver_url

    with pytest.raises(ValueError) as exc_info:
        _parse_sqlserver_url("h;databaseName=db;user=alice;password=topsecret;encrypt=strict")

    assert "topsecret" not in str(exc_info.value)


def test_write_options_no_url_raises():
    """writer() options resolver should raise when url is missing."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    from pysail.spark.datasource.jdbc import JdbcDataSource

    schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
    ds = JdbcDataSource(options={"dbtable": "t"})
    with pytest.raises((ValueError, Exception), match="url"):
        ds.writer(schema, overwrite=False)


def test_write_options_no_dbtable_raises():
    """writer() should reject a query option (can't write to a query)."""
    from pyspark.sql.types import IntegerType, StructField, StructType

    from pysail.spark.datasource.jdbc import JdbcDataSource

    schema = StructType([StructField("id", IntegerType())])
    ds = JdbcDataSource(options={"url": "jdbc:postgresql://localhost:5432/db", "query": "SELECT 1"})
    with pytest.raises((ValueError, Exception), match=r"dbtable|query"):
        ds.writer(schema, overwrite=False)


# ---------------------------------------------------------------------------
# PgWriteEngine identifier + staging name helpers
# ---------------------------------------------------------------------------


def test_quote_qualified_schema_qualified():
    """_quote_qualified handles schema-qualified names (used by the write engine)."""
    from pysail.spark.datasource.jdbc import _quote_qualified

    assert _quote_qualified("public.foo") == '"public"."foo"'
    assert _quote_qualified("foo") == '"foo"'


def test_split_schema():
    """_split_schema separates the schema from the table name."""
    from pysail.spark.datasource.jdbc import _split_schema

    assert _split_schema("orders") == (None, "orders")
    assert _split_schema("public.orders") == ("public", "orders")


def test_pg_write_engine_staging_name():
    """_staging_name_atomic is deterministic and keeps the target's schema."""
    from pysail.spark.datasource.jdbc import _staging_name_atomic

    name = _staging_name_atomic("myschema.orders", "abc123")
    assert name == "myschema.orders__sail_stg_abc123"
    # Deterministic for the same (dbtable, run_id)
    assert _staging_name_atomic("myschema.orders", "abc123") == name
    # An unqualified target stays unqualified
    assert _staging_name_atomic("orders", "abc123") == "orders__sail_stg_abc123"


@pytest.mark.parametrize(
    "helper",
    ["_staging_name_atomic", "_staging_name_truncate_sentinel"],
)
def test_postgres_generated_names_fit_identifier_byte_limit(helper):
    """PostgreSQL limits identifiers by bytes; generated names must not be server-truncated."""
    from pysail.spark.datasource import jdbc

    generated = getattr(jdbc, helper)("public." + ("é" * 31), "0123456789ab")

    schema, table = generated.split(".", 1)
    assert schema == "public"
    assert len(table.encode("utf-8")) <= 63  # PostgreSQL default NAMEDATALEN - 1


@pytest.mark.parametrize(
    "helper",
    ["_staging_name_atomic", "_staging_name_truncate_sentinel"],
)
def test_postgres_generated_names_remain_distinct_after_server_truncation(helper):
    """Run token must survive PostgreSQL's 63-byte truncation."""
    from pysail.spark.datasource import jdbc

    target = "t" * 63
    first = getattr(jdbc, helper)(target, "aaaaaaaaaaaa").encode()[:63]
    second = getattr(jdbc, helper)(target, "bbbbbbbbbbbb").encode()[:63]

    assert first != target.encode()
    assert second != target.encode()
    assert first != second


# ---------------------------------------------------------------------------
# overwrite_mode option resolution
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("stub_target_exists")
def test_overwrite_mode_default_matches_spark():
    """Default overwrite prepares drop/recreate only when execution starts."""
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import JdbcDataSource, JdbcDataSourceWriter

    schema = pa.schema([("id", pa.int32())])
    ds = JdbcDataSource(options={"url": "jdbc:postgresql://localhost:5432/db", "dbtable": "t"})
    writer = ds.writer(schema, overwrite=True)
    assert isinstance(writer, JdbcDataSourceWriter)
    assert writer._engine.overwrite_mode == "append"  # noqa: SLF001


def test_writer_construction_is_database_side_effect_free(monkeypatch):
    import pyarrow as pa

    from pysail.spark.datasource import jdbc

    calls = []
    monkeypatch.setattr(jdbc, "_pg_table_exists", lambda *_a, **_k: calls.append("exists") or True)
    monkeypatch.setattr(jdbc, "_drop_pg_table", lambda *_a, **_k: calls.append("drop"))
    monkeypatch.setattr(jdbc, "_create_pg_table", lambda *_a, **_k: calls.append("create"))
    ds = jdbc.JdbcDataSource(
        options={
            "URL": "jdbc:postgresql://localhost:5432/db",
            "DBTABLE": "t",
            "__sail_save_mode": "overwrite",
        }
    )
    writer = ds.writer(pa.schema([("id", pa.int32())]), overwrite=True)
    assert calls == []
    assert writer._sail_prepare() == "write"  # noqa: SLF001
    assert calls == ["exists", "drop", "create"]


def test_column_resolution_matches_spark_names():
    from pysail.spark.datasource.jdbc import _resolve_column_names

    assert _resolve_column_names(["id", "displayname"], ["ID", "DisplayName", "extra"]) == ["ID", "DisplayName"]
    with pytest.raises(ValueError, match="missing"):
        _resolve_column_names(["missing"], ["id"])
    with pytest.raises(ValueError, match="uniquely"):
        _resolve_column_names(["Id"], ["ID", "id"])


def test_column_resolution_preserves_source_order_and_target_spelling():
    from pysail.spark.datasource.jdbc import _resolve_column_names

    assert _resolve_column_names(
        ["displayname", "id"],
        ["ID", "DisplayName", "unused"],
    ) == ["DisplayName", "ID"]


def test_column_resolution_honors_case_sensitive_analysis():
    from pysail.spark.datasource.jdbc import _resolve_column_names

    assert _resolve_column_names(["ID"], ["ID", "id"], case_sensitive=True) == ["ID"]
    with pytest.raises(ValueError, match="cannot be resolved"):
        _resolve_column_names(["Id"], ["ID"], case_sensitive=True)


def test_unsupported_type_fails_after_missing_target_check(monkeypatch):
    import pyarrow as pa

    from pysail.spark.datasource import jdbc

    calls = []
    monkeypatch.setattr(jdbc, "_sqlalchemy_table_exists", lambda *_a, **_k: calls.append("exists") or False)
    ds = jdbc.JdbcDataSource(options={"url": "jdbc:mysql://localhost/db", "dbtable": "t", "__sail_save_mode": "append"})
    writer = ds.writer(pa.schema([("payload", pa.list_(pa.int32()))]), overwrite=False)
    with pytest.raises(TypeError, match="does not support"):
        writer._sail_prepare()  # noqa: SLF001
    assert calls == ["exists"]


@pytest.mark.usefixtures("stub_target_exists")
def test_errorifexists_rejects_existing_target():
    """The planner-provided default mode must not silently append."""
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import JdbcDataSource

    ds = JdbcDataSource(
        options={
            "url": "jdbc:postgresql://localhost:5432/db",
            "dbtable": "t",
            "__sail_save_mode": "errorifexists",
        }
    )
    writer = ds.writer(pa.schema([("id", pa.int32())]), overwrite=False)
    with pytest.raises(ValueError, match="already exists"):
        writer._sail_prepare()  # noqa: SLF001


@pytest.mark.usefixtures("stub_target_exists")
def test_ignore_existing_target_returns_skip_writer():
    """Ignore mode marks the physical write for complete input elision."""
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import JdbcDataSource

    ds = JdbcDataSource(
        options={
            "url": "jdbc:postgresql://localhost:5432/db",
            "dbtable": "t",
            "__sail_save_mode": "ignore",
        }
    )
    writer = ds.writer(pa.schema([("id", pa.int32())]), overwrite=False)
    assert writer._sail_prepare() == "skip"  # noqa: SLF001


def test_sqlalchemy_truncate_option_preserves_target(monkeypatch):
    """MySQL/SQL Server honor Spark's truncate=true overwrite option."""
    import pyarrow as pa

    from pysail.spark.datasource import jdbc

    reset = []
    monkeypatch.setattr(jdbc, "_sqlalchemy_table_exists", lambda *_a, **_k: True)
    monkeypatch.setattr(jdbc, "_sqlalchemy_target_columns", lambda *_a, **_k: ["id"])
    monkeypatch.setattr(jdbc, "_reset_sqlalchemy_table", lambda *_a, **k: reset.append(k["truncate"]))
    ds = jdbc.JdbcDataSource(
        options={
            "url": "jdbc:mysql://localhost/db",
            "dbtable": "t",
            "truncate": "true",
            "__sail_save_mode": "overwrite",
        }
    )
    writer = ds.writer(pa.schema([("id", pa.int32())]), overwrite=True)
    assert reset == []
    assert writer._sail_prepare() == "write"  # noqa: SLF001
    assert reset == [True]
    assert writer._engine.overwrite is False  # noqa: SLF001


@pytest.mark.parametrize("cascade", ["false", "true"])
def test_postgres_truncate_option_uses_spark_dialect_sql(monkeypatch, cascade):
    import pyarrow as pa

    from pysail.spark.datasource import jdbc

    calls = []
    monkeypatch.setattr(jdbc, "_pg_table_exists", lambda *_a, **_k: True)
    monkeypatch.setattr(jdbc, "_pg_target_columns", lambda *_a, **_k: ["id"])
    monkeypatch.setattr(
        jdbc,
        "_truncate_pg_table",
        lambda *_a, **kwargs: calls.append(kwargs["cascade"]),
    )
    writer = jdbc.JdbcDataSource(
        options={
            "url": "jdbc:postgresql://localhost/db",
            "dbtable": "t",
            "truncate": "true",
            "cascadeTruncate": cascade,
            "__sail_save_mode": "overwrite",
        }
    ).writer(pa.schema([("id", pa.int32())]), overwrite=True)

    assert writer._sail_prepare() == "write"  # noqa: SLF001
    assert calls == [cascade == "true"]


@pytest.mark.usefixtures("stub_target_exists")
def test_truncate_option_rejects_non_boolean():
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import JdbcDataSource

    ds = JdbcDataSource(options={"url": "jdbc:mysql://localhost/db", "dbtable": "t", "truncate": "sometimes"})
    with pytest.raises(ValueError, match="truncate"):
        ds.writer(pa.schema([("id", pa.int32())]), overwrite=True)


@pytest.mark.usefixtures("stub_target_exists")
def test_cascade_truncate_option_rejects_non_boolean():
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import JdbcDataSource

    ds = JdbcDataSource(
        options={
            "url": "jdbc:postgresql://localhost/db",
            "dbtable": "t",
            "truncate": "true",
            "cascadeTruncate": "sometimes",
        }
    )
    with pytest.raises(ValueError, match="cascadeTruncate"):
        ds.writer(pa.schema([("id", pa.int32())]), overwrite=True)


@pytest.mark.usefixtures("stub_target_exists")
def test_overwrite_mode_truncate_valid():
    """The namespaced PostgreSQL truncate extension is accepted."""
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import JdbcDataSource

    schema = pa.schema([("id", pa.int32())])
    ds = JdbcDataSource(
        options={
            "url": "jdbc:postgresql://localhost:5432/db",
            "dbtable": "t",
            "__sail_save_mode": "overwrite",
            "sail.jdbc.overwriteMode": "truncate",
        }
    )
    writer = ds.writer(schema, overwrite=True)
    assert writer._engine.overwrite_mode == "append"  # noqa: SLF001
    assert writer._sail_prepare() == "write"  # noqa: SLF001


def test_overwrite_mode_invalid_raises():
    """Unknown namespaced overwrite mode raises before database access."""
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import JdbcDataSource

    schema = pa.schema([("id", pa.int32())])
    ds = JdbcDataSource(
        options={
            "url": "jdbc:postgresql://localhost:5432/db",
            "dbtable": "t",
            "__sail_save_mode": "overwrite",
            "sail.jdbc.overwriteMode": "badmode",
        }
    )
    with pytest.raises(ValueError, match="badmode"):
        ds.writer(schema, overwrite=True)


@pytest.mark.parametrize(
    ("options", "overwrite", "match"),
    [
        ({"sail.jdbc.overwriteMode": "atomic"}, False, "requires"),
        ({"sail.jdbc.overwriteMode": "atomic", "truncate": "true"}, True, "cannot be combined"),
    ],
)
def test_namespaced_overwrite_mode_rejects_invalid_combinations(options, overwrite, match):
    import pyarrow as pa

    from pysail.spark.datasource.jdbc import JdbcDataSource

    save_mode = "overwrite" if overwrite else "append"
    ds = JdbcDataSource(
        options={
            "URL": "jdbc:postgresql://localhost:5432/db",
            "DBTABLE": "t",
            "__sail_save_mode": save_mode,
            **options,
        }
    )
    with pytest.raises(ValueError, match=match):
        ds.writer(pa.schema([("id", pa.int32())]), overwrite=overwrite)


def test_staging_name_atomic_run_id_suffix():
    """_staging_name_atomic suffixes the run_id so concurrent writers don't collide."""
    from pysail.spark.datasource.jdbc import _staging_name_atomic

    assert _staging_name_atomic("public.orders", "deadbeef") == "public.orders__sail_stg_deadbeef"
    assert _staging_name_atomic("orders", "deadbeef") == "orders__sail_stg_deadbeef"
    # Different run_ids → different staging tables (the concurrency-collision fix)
    assert _staging_name_atomic("orders", "run1") != _staging_name_atomic("orders", "run2")


def test_pg_write_engine_generates_run_id():
    """Each engine instance gets a run_id; passing one is honored."""
    from pysail.spark.datasource.jdbc import PgWriteEngine

    e1 = PgWriteEngine(dsn="postgresql://x/y", dbtable="t", overwrite_mode="atomic")
    e2 = PgWriteEngine(dsn="postgresql://x/y", dbtable="t", overwrite_mode="atomic")
    assert e1.run_id
    assert e2.run_id
    assert e1.run_id != e2.run_id  # distinct engines → distinct run_ids
    assert PgWriteEngine(dsn="postgresql://x/y", dbtable="t", run_id="fixed").run_id == "fixed"


def test_concurrent_engines_have_distinct_staging_names():
    """Two PgWriteEngine instances for the same target produce distinct staging table names.

    This is the pure-unit proof of the run_id fix: no DB required.
    The property ensures concurrent jobs cannot accidentally share a staging table
    and corrupt each other's data.
    """
    from pysail.spark.datasource.jdbc import PgWriteEngine, _staging_name_atomic

    engines = [
        PgWriteEngine(dsn="postgresql://x/y", dbtable="public.orders", overwrite_mode="atomic") for _ in range(8)
    ]

    staging_names = [_staging_name_atomic("public.orders", e.run_id) for e in engines]

    # All run_ids are distinct
    run_ids = [e.run_id for e in engines]
    assert len(set(run_ids)) == len(run_ids), f"duplicate run_ids: {run_ids}"

    # All staging table names are distinct
    assert len(set(staging_names)) == len(staging_names), f"duplicate staging names: {staging_names}"

    # Every staging name encodes the base table name and keeps the schema
    for name in staging_names:
        assert name.startswith("public.orders__sail_stg_")


# ---------------------------------------------------------------------------
# Credential scrubbing: psycopg errors must not leak the DSN
# ---------------------------------------------------------------------------


def test_safe_error_scrubs_password():
    """_safe_error must remove password substrings from exception messages."""
    from pysail.spark.datasource.jdbc import _safe_error

    dsn = "postgresql://user:s3cr3t@myhost:5432/db"
    exc = Exception(f"connection to server at 'myhost' failed: {dsn}: Connection refused")
    result = _safe_error(exc, dsn)
    assert "s3cr3t" not in result
    assert "<dsn-redacted>" in result


def test_atomic_staging_psycopg_error_is_scrubbed():
    """A psycopg failure during atomic staging CREATE is wrapped through _safe_error.

    No raw DSN/password should appear in the raised RuntimeError.
    """
    from unittest import mock

    import psycopg

    from pysail.spark.datasource.jdbc import PgWriteEngine

    dsn = "postgresql://user:t0ps3cr3t@badhost:9999/db"
    engine = PgWriteEngine(dsn=dsn, dbtable="t", overwrite_mode="atomic", run_id="r1")

    with (
        mock.patch("psycopg.connect", side_effect=psycopg.OperationalError(f"could not connect: {dsn}")),
        pytest.raises(RuntimeError) as exc_info,
    ):
        engine.write_partition(0, [])

    err_str = str(exc_info.value)
    assert "t0ps3cr3t" not in err_str, f"Password leaked in error: {err_str!r}"


def test_truncate_advisory_lock_psycopg_error_is_scrubbed():
    """A psycopg failure during truncate-mode advisory lock acquisition is scrubbed."""
    from unittest import mock

    import psycopg

    from pysail.spark.datasource.jdbc import PgWriteEngine

    dsn = "postgresql://user:adv1s0ry@badhost:9999/db"
    engine = PgWriteEngine(dsn=dsn, dbtable="t", overwrite_mode="truncate", run_id="r1")

    with (
        mock.patch("psycopg.connect", side_effect=psycopg.OperationalError(f"could not connect: {dsn}")),
        pytest.raises(RuntimeError) as exc_info,
    ):
        engine.write_partition(0, [])

    err_str = str(exc_info.value)
    assert "adv1s0ry" not in err_str, f"Password leaked in error: {err_str!r}"


def test_commit_atomic_psycopg_error_is_scrubbed():
    """A psycopg failure during commit() atomic rename is scrubbed."""
    from unittest import mock

    import psycopg

    from pysail.spark.datasource.jdbc import PartitionResult, PgWriteEngine

    dsn = "postgresql://user:c0mm1ts3cr3t@badhost:9999/db"
    engine = PgWriteEngine(dsn=dsn, dbtable="t", overwrite_mode="atomic", run_id="r1")
    fake_result = PartitionResult(partition_id=0, rows_written=1, staging_table="t__sail_stg_r1")

    with (
        mock.patch("psycopg.connect", side_effect=psycopg.OperationalError(f"could not connect: {dsn}")),
        pytest.raises(RuntimeError) as exc_info,
    ):
        engine.commit([fake_result])

    err_str = str(exc_info.value)
    assert "c0mm1ts3cr3t" not in err_str, f"Password leaked in error: {err_str!r}"
