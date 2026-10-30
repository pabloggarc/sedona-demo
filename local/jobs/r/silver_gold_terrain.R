# Elevation statistics of the terrain of every municipality of Castilla-La Mancha, computed from
# the Copernicus DEM tiles in bronze with zonal statistics: every pixel that falls inside the
# outline of a municipality counts towards its mean, minimum, maximum and standard deviation. The
# standard deviation is a simple measure of how rugged a municipality is.
#
# Each DEM tile covers 1x1 degree, so it is cut into smaller tiles to spread the work, and a
# municipality usually overlaps several of them. Its statistics are put together from the count,
# sum and variance of every piece.

library(sparklyr)
library(dplyr)

source("jobs/r/lakehouse.R")

dem_path <- "s3a://spatial-lakehouse/bronze/copernicus/dem"

sc <- connect("silver-gold-terrain")

tiles <- spark_read_source(sc, "dem_files", source = "binaryFile", path = dem_path, memory = FALSE) |>
  mutate(file = element_at(split(path, "/"), -1L)) |>
  group_by(file) |>
  dbplyr::window_order(desc(date), desc(hour)) |>
  filter(row_number() == 1) |>
  ungroup() |>
  transmute(tile = explode(RS_Tile(RS_FromGeoTiff(content), 512L, 512L)))

municipalities <- tbl(sc, dbplyr::in_schema("silver", "municipalities")) |>
  filter(region == "ES-CM") |>
  transmute(
    municipality_id,
    name,
    geometry,
    bbox,
    geohash,
    polygon = ST_GeomFromWKB(geometry)
  )

result <- municipalities |>
  inner_join(tiles, sql_on = "RS_Intersects(RHS.tile, LHS.polygon)") |>
  mutate(stats = RS_ZonalStatsAll(tile, polygon, 1L, TRUE)) |>
  filter(stats$count > 0) |>
  group_by(municipality_id, name, geometry, bbox, geohash) |>
  summarise(
    pixels = sum(stats$count),
    total = sum(stats$sum),
    total_squares = sum(stats$count * (stats$variance + stats$mean^2)),
    elevation_min = min(stats$min),
    elevation_max = max(stats$max),
    .groups = "drop"
  ) |>
  mutate(
    elevation_mean = total / pixels,
    elevation_stddev = sqrt(total_squares / pixels - elevation_mean^2)
  ) |>
  transmute(
    municipality_id,
    name,
    elevation_mean = round(elevation_mean, 1),
    elevation_min = round(elevation_min, 1),
    elevation_max = round(elevation_max, 1),
    elevation_stddev = round(elevation_stddev, 1),
    pixels = as.integer64(pixels),
    geometry,
    bbox,
    geohash
  )

explain(result)

merge_into(result, "gold.terrain_municipality", "municipality_id")

spark_disconnect(sc)
