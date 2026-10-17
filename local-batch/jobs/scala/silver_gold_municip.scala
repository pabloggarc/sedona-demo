// Basic example of spatial join: count the observations and species recorded by GBIF in every
// municipality of Castilla-La Mancha. Every occurrence is a point, every municipality a polygon,
// and the join condition is that the polygon contains the point instead of two keys being equal.
// Sedona sees that condition and plans a spatial join, which partitions both sides by location
// so that each point is only checked against the municipalities around it.
//
// Only records of presence identified down to species and placed within 1 km count, since the
// others cannot be counted as a species seen in a given municipality. The result is written to
// gold with the same conventions as silver.

import org.apache.sedona.spark.SedonaContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.{st_constructors => stc, st_predicates => stp}

val sedona = SedonaContext.create(spark)

val occurrences = sedona.read.table("silver.occurrences")
  .where(col("occurrence_status") === "PRESENT")
  .where(col("species").isNotNull)
  .where(
    col("coordinate_uncertainty_m").isNull
      || col("coordinate_uncertainty_m") <= 1000
  )
  .select(
    col("gbif_id"),
    col("species"),
    stc.ST_GeomFromWKB(col("geometry")).as("point")
  )

val municipalities = sedona.read.table("silver.municipalities")
  .where(col("region") === "ES-CM")
  .select(
    col("municipality_id"),
    col("name"),
    col("area_km2"),
    col("geometry"),
    col("bbox"),
    col("geohash"),
    stc.ST_GeomFromWKB(col("geometry")).as("polygon")
  )

val result = municipalities
  .join(occurrences, stp.ST_Contains(col("polygon"), col("point")))
  .groupBy(
    col("municipality_id"),
    col("name"),
    col("area_km2"),
    col("geometry"),
    col("bbox"),
    col("geohash")
  )
  .agg(
    count(col("gbif_id")).as("observations"),
    count_distinct(col("species")).as("species")
  )
  .select(
    col("municipality_id"),
    col("name"),
    round(col("area_km2"), 1).as("area_km2"),
    col("observations"),
    col("species"),
    round(col("observations") / col("area_km2"), 1).as("observations_km2"),
    col("geometry"),
    col("bbox"),
    col("geohash")
  )

result.explain("formatted")

mergeInto(result, "gold.obs_municipality", "municipality_id")

spark.stop()
sys.exit(0)
