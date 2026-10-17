// Elevation statistics of the terrain of every municipality of Castilla-La Mancha, computed from
// the Copernicus DEM tiles in bronze with zonal statistics: every pixel that falls inside the
// outline of a municipality counts towards its mean, minimum, maximum and standard deviation. The
// standard deviation is a simple measure of how rugged a municipality is.
//
// Each DEM tile covers 1x1 degree, so it is cut into smaller tiles to spread the work, and a
// municipality usually overlaps several of them. Its statistics are put together from the count,
// sum and variance of every piece.

import org.apache.sedona.spark.SedonaContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.{st_constructors => stc}

val demPath = "s3a://spatial-lakehouse/bronze/copernicus/dem"

val sedona = SedonaContext.create(spark)

val latestDownload = Window
  .partitionBy(col("file"))
  .orderBy(col("date").desc, col("hour").desc)

val tiles = sedona.read.format("binaryFile").load(demPath)
  .withColumn("file", element_at(split(col("path"), "/"), -1))
  .withColumn("rank", row_number().over(latestDownload))
  .where(col("rank") === 1)
  .select(
    call_function(
      "RS_TileExplode",
      call_function("RS_FromGeoTiff", col("content")),
      lit(512),
      lit(512)
    )
  )

val municipalities = sedona.read.table("silver.municipalities")
  .where(col("region") === "ES-CM")
  .select(
    col("municipality_id"),
    col("name"),
    col("geometry"),
    col("bbox"),
    col("geohash"),
    stc.ST_GeomFromWKB(col("geometry")).as("polygon")
  )

val result = municipalities
  .join(tiles, call_function("RS_Intersects", col("tile"), col("polygon")))
  .withColumn(
    "stats",
    call_function("RS_ZonalStatsAll", col("tile"), col("polygon"), lit(1), lit(true))
  )
  .where(col("stats.count") > 0)
  .groupBy(
    col("municipality_id"),
    col("name"),
    col("geometry"),
    col("bbox"),
    col("geohash")
  )
  .agg(
    sum(col("stats.count")).as("pixels"),
    sum(col("stats.sum")).as("total"),
    sum(col("stats.count") * (col("stats.variance") + pow(col("stats.mean"), 2)))
      .as("total_squares"),
    min(col("stats.min")).as("elevation_min"),
    max(col("stats.max")).as("elevation_max")
  )
  .withColumn("elevation_mean", col("total") / col("pixels"))
  .select(
    col("municipality_id"),
    col("name"),
    round(col("elevation_mean"), 1).as("elevation_mean"),
    round(col("elevation_min"), 1).as("elevation_min"),
    round(col("elevation_max"), 1).as("elevation_max"),
    round(
      sqrt(col("total_squares") / col("pixels") - pow(col("elevation_mean"), 2)), 1
    ).as("elevation_stddev"),
    col("pixels").cast("long").as("pixels"),
    col("geometry"),
    col("bbox"),
    col("geohash")
  )

result.explain("formatted")

mergeInto(result, "gold.terrain_municipality", "municipality_id")

spark.stop()
sys.exit(0)
