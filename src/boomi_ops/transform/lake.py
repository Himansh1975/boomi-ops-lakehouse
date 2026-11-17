"""Spark session + where Delta tables live.

Locally: Delta tables are folders under <data_root>/lake. On Databricks the session already has
Delta. Moving to Unity Catalog tables (catalog.schema.table) only needs changes in this file.
"""
from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def get_spark(app: str = "boomi-ops") -> SparkSession:
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return SparkSession.builder.getOrCreate()
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "2g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


class Lake:
    def __init__(self, spark: SparkSession, data_root: str | Path):
        self.spark = spark
        self.root = Path(data_root)
        self.landing = self.root / "landing"

    def path(self, table: str) -> str:
        return str(self.root / "lake" / table)

    def exists(self, table: str) -> bool:
        from delta.tables import DeltaTable

        return DeltaTable.isDeltaTable(self.spark, self.path(table))

    def read(self, table: str) -> DataFrame:
        return self.spark.read.format("delta").load(self.path(table))

    def delta(self, table: str):
        from delta.tables import DeltaTable

        return DeltaTable.forPath(self.spark, self.path(table))

    def overwrite(self, df: DataFrame, table: str) -> None:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(self.path(table))

    def append(self, df: DataFrame, table: str) -> None:
        df.write.format("delta").mode("append").save(self.path(table))
