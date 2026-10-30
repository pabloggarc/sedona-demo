"""
Basic example of spatial join: count the observations and species recorded by GBIF in every
municipality of Castilla-La Mancha. Every occurrence is a point, every municipality a polygon,
and the join condition is that the polygon contains the point instead of two keys being equal.
Sedona sees that condition and plans a spatial join, which partitions both sides by location
so that each point is only checked against the municipalities around it.

Only records of presence identified down to species and placed within 1 km count, since the
others cannot be counted as a species seen in a given municipality. The result is written to
gold with the same conventions as silver.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_predicates as stp

from lakehouse import merge_into

spark = SparkSession.builder.appName("silver-gold-municip").getOrCreate()
sedona = SedonaContext.create(spark)

df_occurrences = sedona.read.table("silver.occurrences") \
    .where(F.col("occurrence_status") == "PRESENT") \
    .where(F.col("species").isNotNull()) \
    .where(
        F.col("coordinate_uncertainty_m").isNull()
        | (F.col("coordinate_uncertainty_m") <= 1000)
    ) \
    .select(
        F.col("gbif_id"),
        F.col("species"),
        stc.ST_GeomFromWKB(F.col("geometry")).alias("point"),
    )

df_municipalities = sedona.read.table("silver.municipalities") \
    .where(F.col("region") == "ES-CM") \
    .select(
        F.col("municipality_id"),
        F.col("name"),
        F.col("area_km2"),
        F.col("geometry"),
        F.col("bbox"),
        F.col("geohash"),
        stc.ST_GeomFromWKB(F.col("geometry")).alias("polygon"),
    )

df_result = df_municipalities \
    .join(
        other=df_occurrences,
        on=stp.ST_Contains(F.col("polygon"), F.col("point")),
    ) \
    .groupBy(
        F.col("municipality_id"),
        F.col("name"),
        F.col("area_km2"),
        F.col("geometry"),
        F.col("bbox"),
        F.col("geohash"),
    ) \
    .agg(
        F.count(F.col("gbif_id")).alias("observations"),
        F.count_distinct(F.col("species")).alias("species"),
    ) \
    .select(
        F.col("municipality_id"),
        F.col("name"),
        F.round(F.col("area_km2"), 1).alias("area_km2"),
        F.col("observations"),
        F.col("species"),
        F.round(F.col("observations") / F.col("area_km2"), 1).alias("observations_km2"),
        F.col("geometry"),
        F.col("bbox"),
        F.col("geohash"),
    )

df_result.explain(mode="formatted")

merge_into(df_result, "gold.obs_municipality", "municipality_id")
