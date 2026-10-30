// Merge the downloads of the last days in bronze into Iceberg tables in silver, keyed by the id of
// each record. When several downloads hold the same record, the most recent one wins.
//
// Silver keeps every record, cleaned and typed but not filtered for any particular analysis, and
// all its tables follow the same conventions for the spatial part:
//
// - Coordinates in longitude and latitude (WGS84).
// - The shape in a `geometry` column as WKB, the standard binary encoding for geometries. Iceberg
//   has its own geometry type, but Sedona cannot write it yet.
// - The bounding box of each shape in a `bbox` struct, so that queries filtering by area can skip
//   the files that do not overlap it.
// - Rows sorted by geohash, so that nearby rows end up in the same files and those file-level
//   bounding boxes stay small.

import org.apache.sedona.spark.SedonaContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.{st_constructors => stc, st_functions => stf}

val lookbackDays = 365

val sedona = SedonaContext.create(spark)

val latestDownload = Window
  .partitionBy(col("id"))
  .orderBy(col("date").desc, col("hour").desc)

val occurrences = sedona.read.table("bronze.gbif_occurrence")
  .where(col("date") >= date_sub(current_date(), lookbackDays))
  .withColumn("id", col("gbifid").cast("long"))
  .withColumn("rank", row_number().over(latestDownload))
  .where(col("rank") === 1)
  .withColumn("point", stc.ST_Point(col("decimallongitude"), col("decimallatitude")))
  .select(
    col("id").as("gbif_id"),
    col("occurrencestatus").as("occurrence_status"),
    col("basisofrecord").as("basis_of_record"),
    col("taxonrank").as("taxon_rank"),
    col("scientificname").as("scientific_name"),
    col("kingdom"),
    col("phylum"),
    col("class"),
    col("order"),
    col("family"),
    col("genus"),
    col("species"),
    try_to_date(substring(col("eventdate"), 1, 10), "yyyy-MM-dd").as("event_date"),
    col("year"),
    col("month"),
    col("coordinateuncertaintyinmeters").as("coordinate_uncertainty_m"),
    col("datasetkey").as("dataset_key"),
    stf.ST_AsBinary(col("point")).as("geometry"),
    struct(
      stf.ST_XMin(col("point")).as("xmin"),
      stf.ST_YMin(col("point")).as("ymin"),
      stf.ST_XMax(col("point")).as("xmax"),
      stf.ST_YMax(col("point")).as("ymax")
    ).as("bbox"),
    stf.ST_GeoHash(col("point"), lit(6)).as("geohash")
  )

mergeInto(occurrences, "silver.occurrences", "gbif_id")

val municipalities = sedona.read.table("bronze.overture_division_area")
  .where(col("date") >= date_sub(current_date(), lookbackDays))
  .where(col("subtype") === "locality")
  .withColumn("rank", row_number().over(latestDownload))
  .where(col("rank") === 1)
  .select(
    col("id").as("municipality_id"),
    col("names.primary").as("name"),
    col("country"),
    col("region"),
    (stf.ST_AreaSpheroid(col("geometry")) / 1e6).as("area_km2"),
    stf.ST_AsBinary(col("geometry")).as("geometry"),
    struct(
      stf.ST_XMin(col("geometry")).as("xmin"),
      stf.ST_YMin(col("geometry")).as("ymin"),
      stf.ST_XMax(col("geometry")).as("xmax"),
      stf.ST_YMax(col("geometry")).as("ymax")
    ).as("bbox"),
    stf.ST_GeoHash(stf.ST_Centroid(col("geometry")), lit(6)).as("geohash")
  )

mergeInto(municipalities, "silver.municipalities", "municipality_id")

spark.stop()
sys.exit(0)
