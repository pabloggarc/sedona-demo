"""
Helpers shared by the jobs that write to the lakehouse.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def merge_into(df: DataFrame, table: str, key: str) -> None:
    """
    Merge a DataFrame into an Iceberg table by key, creating the table first if it does not
    exist. New tables keep their rows sorted by the ``geohash`` column.

    Args:
        df (DataFrame): Rows to merge, with one row per key.
        table (str): Name of the table, e.g. ``silver.occurrences``.
        key (str): Column that identifies a record.
    """
    spark = df.sparkSession
    if not spark.catalog.tableExists(table):
        df.limit(0).writeTo(table).using("iceberg") \
            .tableProperty("write.distribution-mode", "range") \
            .create()
        spark.sql(f"ALTER TABLE spark_catalog.{table} WRITE ORDERED BY geohash")

    df.alias("source") \
        .mergeInto(table, F.col(f"source.{key}") == F.col(f"{table.split('.')[1]}.{key}")) \
        .whenMatched().updateAll() \
        .whenNotMatched().insertAll() \
        .merge()
