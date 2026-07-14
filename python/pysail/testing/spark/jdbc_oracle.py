"""Opt-in native Spark 4.1.2 JDBC writer oracle for differential tests."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

_DRIVER_PACKAGES = {
    "postgresql": "org.postgresql:postgresql:42.7.7",
    "mysql": "com.mysql:mysql-connector-j:9.4.0",
    "sqlserver": "com.microsoft.sqlserver:mssql-jdbc:12.10.2.jre11",
}

_DRIVER_CLASSES = {
    "postgresql": "org.postgresql.Driver",
    "mysql": "com.mysql.cj.jdbc.Driver",
    "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}


def native_spark_4_1_2_python() -> Path | None:
    """Return the configured isolated Spark 4.1.2 interpreter, if available."""
    value = os.environ.get("SAIL_SPARK_4_1_2_PYTHON")
    return Path(value) if value else None


def run_native_jdbc_write(
    *,
    dialect: str,
    jdbc_url: str,
    dbtable: str,
    user: str,
    password: str,
    schema_json: dict,
    rows: list[list[object]],
    mode: str | None,
    options: dict[str, str] | None = None,
) -> None:
    """Run one JDBC write in an isolated native Spark 4.1.2 process."""
    python = native_spark_4_1_2_python()
    if python is None:
        msg = "SAIL_SPARK_4_1_2_PYTHON is required for native Spark differential tests"
        raise RuntimeError(msg)
    payload = {
        "jdbc_url": jdbc_url,
        "dbtable": dbtable,
        "user": user,
        "password": password,
        "schema": schema_json,
        "rows": rows,
        "mode": mode,
        "options": options or {},
        "package": _DRIVER_PACKAGES[dialect],
        "driver": _DRIVER_CLASSES[dialect],
    }
    code = """
import json, os, sys
payload = json.loads(sys.stdin.read())
os.environ.pop("SPARK_REMOTE", None)
os.environ.pop("SPARK_CONNECT_MODE_ENABLED", None)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
spark = (SparkSession.builder.master("local[2]")
         .appName("sail-jdbc-spark-oracle")
         .config("spark.ui.enabled", "false")
         .config("spark.jars.packages", payload["package"])
         .getOrCreate())
try:
    if spark.version != "4.1.2":
        raise RuntimeError(f"Expected Spark 4.1.2, got {spark.version}")
    df = spark.createDataFrame(payload["rows"], StructType.fromJson(payload["schema"]))
    writer = (df.write.format("jdbc")
              .option("url", payload["jdbc_url"])
              .option("dbtable", payload["dbtable"])
              .option("user", payload["user"])
              .option("password", payload["password"])
              .option("driver", payload["driver"])
              .options(**payload["options"]))
    if payload["mode"] is not None:
        writer = writer.mode(payload["mode"])
    writer.save()
finally:
    spark.stop()
"""
    env = os.environ.copy()
    env["PYSPARK_PYTHON"] = str(python)
    env["PYSPARK_DRIVER_PYTHON"] = str(python)
    subprocess.run(
        [str(python), "-c", code],
        input=json.dumps(payload),
        text=True,
        check=True,
        env=env,
    )
