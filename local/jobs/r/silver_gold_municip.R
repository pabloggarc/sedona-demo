# Basic example of spatial join: count the observations and species recorded by GBIF in every
# municipality of Castilla-La Mancha. Every occurrence is a point, every municipality a polygon,
# and the join condition is that the polygon contains the point instead of two keys being equal.
# Sedona sees that condition and plans a spatial join, which partitions both sides by location
# so that each point is only checked against the municipalities around it.
#
# Only records of presence identified down to species and placed within 1 km count, since the
# others cannot be counted as a species seen in a given municipality. The result is written to
# gold with the same conventions as silver.

library(sparklyr)
library(dplyr)

source("jobs/r/lakehouse.R")

sc <- connect("silver-gold-municip")

occurrences <- tbl(sc, dbplyr::in_schema("silver", "occurrences")) |>
  filter(
    occurrence_status == "PRESENT",
    !is.na(species),
    is.na(coordinate_uncertainty_m) | coordinate_uncertainty_m <= 1000
  ) |>
  transmute(gbif_id, species, point = ST_GeomFromWKB(geometry))

municipalities <- tbl(sc, dbplyr::in_schema("silver", "municipalities")) |>
  filter(region == "ES-CM") |>
  transmute(
    municipality_id,
    name,
    area_km2,
    geometry,
    bbox,
    geohash,
    polygon = ST_GeomFromWKB(geometry)
  )

result <- municipalities |>
  inner_join(occurrences, sql_on = "ST_Contains(LHS.polygon, RHS.point)") |>
  group_by(municipality_id, name, area_km2, geometry, bbox, geohash) |>
  summarise(
    observations = n(),
    species = n_distinct(species),
    .groups = "drop"
  ) |>
  mutate(
    observations_km2 = round(observations / area_km2, 1),
    area_km2 = round(area_km2, 1)
  ) |>
  select(
    municipality_id,
    name,
    area_km2,
    observations,
    species,
    observations_km2,
    geometry,
    bbox,
    geohash
  )

explain(result)

merge_into(result, "gold.obs_municipality", "municipality_id")

spark_disconnect(sc)
