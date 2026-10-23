package sedonademo

import org.apache.sedona.spark.SedonaContext
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.{
  st_constructors => stc,
  st_functions => stf,
  st_predicates => stp
}

/** Counts the observations and species recorded by GBIF in every municipality, straight from the
  * public snapshots of GBIF and Overture on S3.
  *
  * It is the same spatial join as the local demo, at the size of the planet.
  * The polygons do not fit in the memory of a single executor any more,
  * so instead of broadcasting them, Sedona partitions both sides by
  * location and builds an index in each partition.
  *
  * The only argument is the scope, either a country code such as ES, to try it on a single
  * country, or world. The result replaces the rows of that scope in gold.obs_municipality.
  */
object ObsMunicipality {

  def main(args: Array[String]): Unit = {
    val scope = args(0).toLowerCase

    val spark = SparkSession.builder()
      .appName(s"obs-municipality-$scope")
      .config("spark.sedona.join.spatitionside", "right")
      .enableHiveSupport()
      .getOrCreate()

    val sedona = SedonaContext.create(spark)

    def inScope(country: String): Column =
      if (scope == "world") lit(true) else col(country) === scope.toUpperCase

    val occurrences = sedona.read.table("bronze.gbif_occurrence")
      .where(col("occurrencestatus") === "PRESENT")
      .where(col("species").isNotNull)
      .where(
        col("coordinateuncertaintyinmeters").isNull
          || col("coordinateuncertaintyinmeters") <= 1000
      )
      .where(inScope("countrycode"))

    val municipalities = sedona.read.table("bronze.overture_division_area")
      .where(col("subtype") === "locality")
      .where(inScope("country"))

    val points = occurrences.select(
      col("species"),
      stc.ST_Point(col("decimallongitude"), col("decimallatitude")).as("point")
    )

    // Checking whether a point is inside a polygon walks the whole outline, and a few coastal
    // municipalities have tens of thousands of vertices. Each one is cut into pieces of 256 vertices
    // at most, and every piece keeps the id of its municipality.
    val polygons = municipalities.select(
      col("id").as("municipality_id"),
      stf.ST_SubDivideExplode(stc.ST_GeomFromWKB(col("geometry")), lit(256)).as("polygon")
    )

    // Only the id goes through the spatial join and the aggregation. The name and the outline come
    // back afterwards with a plain join, instead of shuffling lots of polygons around.
    //
    // The order of the join matters. The grid is drawn from the right side (see spatitionside
    // above), and every task builds its index from the left side, so the few polygons go to the
    // index and the many points are only streamed through it.
    val counts = polygons
      .join(points, stp.ST_Contains(col("polygon"), col("point")))
      .groupBy(col("municipality_id"))
      .agg(
        count("*").as("observations"),
        count_distinct(col("species")).as("species")
      )

    val result = municipalities
      .select(
        lit(scope).as("scope"),
        col("id").as("municipality_id"),
        col("names.primary").as("name"),
        col("country"),
        col("region"),
        col("geometry")
      )
      .join(counts, "municipality_id")

    result.writeTo("gold.obs_municipality").overwrite(col("scope") === scope)

    spark.stop()
  }
}
