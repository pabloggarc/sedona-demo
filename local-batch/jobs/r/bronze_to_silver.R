# Merge the downloads of the last days in bronze into Iceberg tables in silver, keyed by the id of
# each record. When several downloads hold the same record, the most recent one wins.
#
# Silver keeps every record, cleaned and typed but not filtered for any particular analysis, and
# all its tables follow the same conventions for the spatial part:
#
# - Coordinates in longitude and latitude (WGS84).
# - The shape in a `geometry` column as WKB, the standard binary encoding for geometries. Iceberg
#   has its own geometry type, but Sedona cannot write it yet.
# - The bounding box of each shape in a `bbox` struct, so that queries filtering by area can skip
#   the files that do not overlap it.
# - Rows sorted by geohash, so that nearby rows end up in the same files and those file-level
#   bounding boxes stay small.

library(sparklyr)
library(dplyr)

source("jobs/r/lakehouse.R")

lookback_days <- 365L

sc <- connect("bronze-to-silver")

occurrences <- tbl(sc, dbplyr::in_schema("bronze", "gbif_occurrence")) |>
  filter(date >= date_sub(current_date(), lookback_days)) |>
  mutate(id = as.integer64(gbifid)) |>
  group_by(id) |>
  dbplyr::window_order(desc(date), desc(hour)) |>
  filter(row_number() == 1) |>
  ungroup() |>
  mutate(point = ST_Point(decimallongitude, decimallatitude)) |>
  transmute(
    gbif_id = id,
    occurrence_status = occurrencestatus,
    basis_of_record = basisofrecord,
    taxon_rank = taxonrank,
    scientific_name = scientificname,
    kingdom,
    phylum,
    class,
    order,
    family,
    genus,
    species,
    event_date = try_to_date(substr(eventdate, 1, 10), "yyyy-MM-dd"),
    year,
    month,
    coordinate_uncertainty_m = coordinateuncertaintyinmeters,
    dataset_key = datasetkey,
    geometry = ST_AsBinary(point),
    bbox = named_struct(
      "xmin", ST_XMin(point),
      "ymin", ST_YMin(point),
      "xmax", ST_XMax(point),
      "ymax", ST_YMax(point)
    ),
    geohash = ST_GeoHash(point, 6L)
  )

merge_into(occurrences, "silver.occurrences", "gbif_id")

municipalities <- tbl(sc, dbplyr::in_schema("bronze", "overture_division_area")) |>
  filter(date >= date_sub(current_date(), lookback_days), subtype == "locality") |>
  group_by(id) |>
  dbplyr::window_order(desc(date), desc(hour)) |>
  filter(row_number() == 1) |>
  ungroup() |>
  transmute(
    municipality_id = id,
    name = names$primary,
    country,
    region,
    area_km2 = ST_AreaSpheroid(geometry) / 1e6,
    bbox = named_struct(
      "xmin", ST_XMin(geometry),
      "ymin", ST_YMin(geometry),
      "xmax", ST_XMax(geometry),
      "ymax", ST_YMax(geometry)
    ),
    geohash = ST_GeoHash(ST_Centroid(geometry), 6L),
    geometry = ST_AsBinary(geometry)
  ) |>
  select(municipality_id, name, country, region, area_km2, geometry, bbox, geohash)

merge_into(municipalities, "silver.municipalities", "municipality_id")

spark_disconnect(sc)
