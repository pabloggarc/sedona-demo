"""
Merge the downloads of the last days in bronze into Iceberg tables in silver, keyed by the id of
each record. When several downloads hold the same record, the most recent one wins.

Silver keeps every record, cleaned and typed but not filtered for any particular analysis, and
all its tables follow the same conventions for the spatial part:

- Coordinates in longitude and latitude (WGS84).
- The shape in a ``geometry`` column as WKB, the standard binary encoding for geometries. Iceberg
  has its own geometry type, but Sedona cannot write it yet.
- The bounding box of each shape in a ``bbox`` struct, so that queries filtering by area can skip
  the files that do not overlap it.
- Rows sorted by geohash, so that nearby rows end up in the same files and those file-level
  bounding boxes stay small.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from lakehouse import merge_into

LOOKBACK_DAYS = 365

spark = SparkSession.builder.appName("bronze-to-silver").getOrCreate()
sedona = SedonaContext.create(spark)

latest_download = Window \
    .partitionBy(F.col("id")) \
    .orderBy(F.col("date").desc(), F.col("hour").desc())

df_occurrences = sedona.read.table("bronze.gbif_occurrence") \
    .where(F.col("date") >= F.date_sub(F.current_date(), LOOKBACK_DAYS)) \
    .withColumn("id", F.col("gbifid").cast("long")) \
    .withColumn("rank", F.row_number().over(latest_download)) \
    .where(F.col("rank") == 1) \
    .withColumn("point", stc.ST_Point(F.col("decimallongitude"), F.col("decimallatitude"))) \
    .select(
        F.col("id").alias("gbif_id"),
        F.col("occurrencestatus").alias("occurrence_status"),
        F.col("basisofrecord").alias("basis_of_record"),
        F.col("taxonrank").alias("taxon_rank"),
        F.col("scientificname").alias("scientific_name"),
        F.col("kingdom"),
        F.col("phylum"),
        F.col("class"),
        F.col("order"),
        F.col("family"),
        F.col("genus"),
        F.col("species"),
        F.try_to_date(F.substring(F.col("eventdate"), 1, 10), "yyyy-MM-dd").alias("event_date"),
        F.col("year"),
        F.col("month"),
        F.col("coordinateuncertaintyinmeters").alias("coordinate_uncertainty_m"),
        F.col("datasetkey").alias("dataset_key"),
        stf.ST_AsBinary(F.col("point")).alias("geometry"),
        F.struct(
            stf.ST_XMin(F.col("point")).alias("xmin"),
            stf.ST_YMin(F.col("point")).alias("ymin"),
            stf.ST_XMax(F.col("point")).alias("xmax"),
            stf.ST_YMax(F.col("point")).alias("ymax"),
        ).alias("bbox"),
        stf.ST_GeoHash(F.col("point"), 6).alias("geohash"),
    )

merge_into(df_occurrences, "silver.occurrences", "gbif_id")

df_municipalities = sedona.read.table("bronze.overture_division_area") \
    .where(F.col("date") >= F.date_sub(F.current_date(), LOOKBACK_DAYS)) \
    .where(F.col("subtype") == "locality") \
    .withColumn("rank", F.row_number().over(latest_download)) \
    .where(F.col("rank") == 1) \
    .select(
        F.col("id").alias("municipality_id"),
        F.col("names.primary").alias("name"),
        F.col("country"),
        F.col("region"),
        (stf.ST_AreaSpheroid(F.col("geometry")) / 1e6).alias("area_km2"),
        stf.ST_AsBinary(F.col("geometry")).alias("geometry"),
        F.struct(
            stf.ST_XMin(F.col("geometry")).alias("xmin"),
            stf.ST_YMin(F.col("geometry")).alias("ymin"),
            stf.ST_XMax(F.col("geometry")).alias("xmax"),
            stf.ST_YMax(F.col("geometry")).alias("ymax"),
        ).alias("bbox"),
        stf.ST_GeoHash(stf.ST_Centroid(F.col("geometry")), 6).alias("geohash"),
    )

merge_into(df_municipalities, "silver.municipalities", "municipality_id")
