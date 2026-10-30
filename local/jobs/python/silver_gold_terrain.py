"""
Elevation statistics of the terrain of every municipality of Castilla-La Mancha, computed from
the Copernicus DEM tiles in bronze with zonal statistics: every pixel that falls inside the
outline of a municipality counts towards its mean, minimum, maximum and standard deviation. The
standard deviation is a simple measure of how rugged a municipality is.

Each DEM tile covers 1x1 degree, so it is cut into smaller tiles to spread the work, and a
municipality usually overlaps several of them. Its statistics are put together from the count,
sum and variance of every piece.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc

from lakehouse import merge_into

DEM_PATH = "s3a://spatial-lakehouse/bronze/copernicus/dem"

spark = SparkSession.builder.appName("silver-gold-terrain").getOrCreate()
sedona = SedonaContext.create(spark)

latest_download = Window \
    .partitionBy(F.col("file")) \
    .orderBy(F.col("date").desc(), F.col("hour").desc())

df_tiles = sedona.read.format("binaryFile").load(DEM_PATH) \
    .withColumn("file", F.element_at(F.split(F.col("path"), "/"), -1)) \
    .withColumn("rank", F.row_number().over(latest_download)) \
    .where(F.col("rank") == 1) \
    .select(
        F.call_function(
            "RS_TileExplode",
            F.call_function("RS_FromGeoTiff", F.col("content")),
            F.lit(512),
            F.lit(512),
        ),
    )

df_municipalities = sedona.read.table("silver.municipalities") \
    .where(F.col("region") == "ES-CM") \
    .select(
        F.col("municipality_id"),
        F.col("name"),
        F.col("geometry"),
        F.col("bbox"),
        F.col("geohash"),
        stc.ST_GeomFromWKB(F.col("geometry")).alias("polygon"),
    )

df_result = df_municipalities \
    .join(
        other=df_tiles,
        on=F.call_function("RS_Intersects", F.col("tile"), F.col("polygon")),
    ) \
    .withColumn(
        "stats",
        F.call_function("RS_ZonalStatsAll", F.col("tile"), F.col("polygon"), F.lit(1), F.lit(True)),
    ) \
    .where(F.col("stats.count") > 0) \
    .groupBy(
        F.col("municipality_id"),
        F.col("name"),
        F.col("geometry"),
        F.col("bbox"),
        F.col("geohash"),
    ) \
    .agg(
        F.sum(F.col("stats.count")).alias("pixels"),
        F.sum(F.col("stats.sum")).alias("total"),
        F.sum(
            F.col("stats.count") * (F.col("stats.variance") + F.col("stats.mean") ** 2)
        ).alias("total_squares"),
        F.min(F.col("stats.min")).alias("elevation_min"),
        F.max(F.col("stats.max")).alias("elevation_max"),
    ) \
    .withColumn("elevation_mean", F.col("total") / F.col("pixels")) \
    .select(
        F.col("municipality_id"),
        F.col("name"),
        F.round(F.col("elevation_mean"), 1).alias("elevation_mean"),
        F.round(F.col("elevation_min"), 1).alias("elevation_min"),
        F.round(F.col("elevation_max"), 1).alias("elevation_max"),
        F.round(
            F.sqrt(F.col("total_squares") / F.col("pixels") - F.col("elevation_mean") ** 2), 1
        ).alias("elevation_stddev"),
        F.col("pixels").cast("long").alias("pixels"),
        F.col("geometry"),
        F.col("bbox"),
        F.col("geohash"),
    )

df_result.explain(mode="formatted")

merge_into(df_result, "gold.terrain_municipality", "municipality_id")
