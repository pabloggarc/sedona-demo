# Helpers shared by the jobs that write to the lakehouse.

#' Connect to Spark. Sedona, the metastore and RustFS are already set up in spark-defaults.conf,
#' so the apache.sedona package is not needed.
#'
#' @param app_name Name of the Spark application.
#' @return A Spark connection.
connect <- function(app_name) {
  # In local mode sparklyr passes --driver-memory 2g, which wins over spark-defaults.conf
  config <- sparklyr::spark_config()
  config[["sparklyr.shell.driver-memory"]] <- "6g"

  sparklyr::spark_connect(
    master = "local[*]",
    spark_home = Sys.getenv("SPARK_HOME"),
    app_name = app_name,
    config = config
  )
}

#' Merge a table into an Iceberg table by key, creating the table first if it does not exist.
#' New tables keep their rows sorted by the `geohash` column.
#'
#' @param df Rows to merge, with one row per key.
#' @param table Name of the table, e.g. `silver.occurrences`.
#' @param key Column that identifies a record.
merge_into <- function(df, table, key) {
  sc <- sparklyr::spark_connection(df)
  dplyr::compute(df, name = "source")

  parts <- strsplit(table, ".", fixed = TRUE)[[1]]
  existing <- DBI::dbGetQuery(sc, sprintf("SHOW TABLES IN %s LIKE '%s'", parts[1], parts[2]))
  if (nrow(existing) == 0) {
    DBI::dbExecute(sc, sprintf(
      "CREATE TABLE spark_catalog.%s USING iceberg
       TBLPROPERTIES ('write.distribution-mode' = 'range')
       AS SELECT * FROM source LIMIT 0",
      table
    ))
    DBI::dbExecute(sc, sprintf("ALTER TABLE spark_catalog.%s WRITE ORDERED BY geohash", table))
  }

  DBI::dbExecute(sc, sprintf(
    "MERGE INTO spark_catalog.%s target
     USING source
     ON target.%s = source.%s
     WHEN MATCHED THEN UPDATE SET *
     WHEN NOT MATCHED THEN INSERT *",
    table, key, key
  ))
  sparklyr::tbl_uncache(sc, "source")
}
