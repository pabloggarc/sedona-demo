package sedonademo

import org.apache.sedona.spark.SedonaContext
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.{
  st_constructors => stc,
  st_functions => stf,
  st_predicates => stp
}
import org.apache.spark.storage.StorageLevel

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

  val PointsPerCell = 20000L

  def main(args: Array[String]): Unit = {
    val scope = args(0).toLowerCase

    val spark = SparkSession.builder()
      .appName(s"obs-municipality-$scope")
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
      .where(col("decimallongitude").isNotNull && col("decimallatitude").isNotNull)

    val municipalities = sedona.read.table("bronze.overture_division_area")
      .where(col("subtype") === "locality")
      .where(inScope("country"))

    // Many observations share the same coordinates, from the hotspots of eBird to coordinates
    // rounded to the center of a region, and Sedona cannot split a cell full of identical points.
    // Counting them by coordinate and species first leaves each coordinate repeated at most once
    // per species. It is kept on disk if it does not fit in memory, since the count below and
    // Sedona go through it many times.
    val observations = occurrences
      .groupBy(col("decimallongitude"), col("decimallatitude"), col("species"))
      .agg(count("*").as("observations"))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val points = observations.select(
      col("species"),
      col("observations"),
      stc.ST_Point(col("decimallongitude"), col("decimallatitude")).as("point")
    )

    // Checking whether a point is inside a polygon walks the whole outline, and a few coastal
    // municipalities have tens of thousands of vertices. Each one is cut into pieces of 256
    // vertices at most, and every piece keeps the id of its municipality.
    // Overture is read in a few dozen tasks, so the polygons are spread over the cluster first,
    // or a handful of tasks would cut them all.
    val polygons = municipalities.repartition(1000)
      .select(
        col("id").as("municipality_id"),
        stf.ST_SubDivideExplode(stc.ST_GeomFromWKB(col("geometry")), lit(256)).as("polygon")
      )

    // Sedona splits the map into cells drawn from a sample of one side of the join, so that every
    // cell gets about the same number of its elements, and every cell builds an index from the left
    // side. The polygons go on the left, so they are the ones indexed, and the grid is drawn from
    // the points on the right, so the cells follow where the observations are. On its own, Sedona
    // makes as many cells as partitions there are on that side, which has nothing to do with the
    // number of points, so there are enough cells for PointsPerCell points in each.
    val cells = math.max(1L, observations.count() / PointsPerCell)
    println(s"spatial join with $cells cells")
    spark.conf.set("spark.sedona.join.spatitionside", "right")
    spark.conf.set("spark.sedona.join.numpartition", cells)

    // Only the id goes through the spatial join and the aggregation. The name and the outline come
    // back afterwards with a plain join, instead of shuffling lots of polygons around.
    val counts = polygons
      .join(points, stp.ST_Contains(col("polygon"), col("point")))
      .groupBy(col("municipality_id"))
      .agg(
        sum(col("observations")).as("observations"),
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
