// Helpers shared by the jobs that write to the lakehouse. spark-shell loads this file before the
// job with an extra -i, since scripts run with -i cannot use :load.

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/** Merges a DataFrame into an Iceberg table by key, creating the table first if it does not
  * exist. New tables keep their rows sorted by the `geohash` column.
  *
  * @param df    rows to merge, with one row per key
  * @param table name of the table, e.g. `silver.occurrences`
  * @param key   column that identifies a record
  */
def mergeInto(df: DataFrame, table: String, key: String): Unit = {
  val spark = df.sparkSession
  if (!spark.catalog.tableExists(table)) {
    df.limit(0).writeTo(table).using("iceberg")
      .tableProperty("write.distribution-mode", "range")
      .create()
    spark.sql(s"ALTER TABLE spark_catalog.$table WRITE ORDERED BY geohash")
  }

  val name = table.split('.')(1)
  df.as("source")
    .mergeInto(table, col(s"source.$key") === col(s"$name.$key"))
    .whenMatched().updateAll()
    .whenNotMatched().insertAll()
    .merge()
}
