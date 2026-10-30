#!/usr/bin/env bash
# Downloads the Castilla-La Mancha slice of the data into data/raw. Files that are already there
# are skipped, so it can be run again after a failure.
set -euo pipefail
cd "$(dirname "$0")/.."

# Bounding box of Castilla-La Mancha (xmin,ymin,xmax,ymax) and its GADM id, which GBIF uses to
# tag every occurrence with the region it falls in
BBOX="-5.41,38.02,-0.91,41.33"
export GADM_GID="ESP.4_1"

# Overture only keeps its latest releases, see https://docs.overturemaps.org/release-calendar/
OVERTURE_RELEASE="2026-09-23.1"

if [[ ! -f .env ]]; then
  echo "Missing .env, copy .env.example and fill in your GBIF account" >&2
  exit 1
fi
set -a; source .env; set +a

mkdir -p data/raw/{overture,dem,gbif}

echo "Downloading Overture division areas..."
out=data/raw/overture/division_area.parquet
if [[ ! -f $out ]]; then
  uvx overturemaps@1.0.2 download -t division_area -f geoparquet \
    -r "$OVERTURE_RELEASE" --bbox="$BBOX" -o "$out.tmp"
  mv "$out.tmp" "$out"
  rm -f "$out.tmp.state"
fi

echo "Downloading Copernicus DEM tiles..."
# Tiles cover 1x1 degree and are named after their south-west corner, so N40_00_W004 goes from
# 40N to 41N and from 4W to 3W. Only northern latitudes and western longitudes are handled.
floor() { awk -v x="$1" 'BEGIN { f = int(x); if (f > x) f--; print f }'; }
IFS=, read -r xmin ymin xmax ymax <<< "$BBOX"
for lat in $(seq "$(floor "$ymin")" "$(floor "$ymax")"); do
  for lon in $(seq "$(floor "$xmin")" "$(floor "$xmax")"); do
    tile=$(printf "Copernicus_DSM_COG_10_N%02d_00_W%03d_00_DEM" "$lat" "$((-lon))")
    out=data/raw/dem/$tile.tif
    if [[ ! -f $out ]]; then
      curl -fsS --retry 3 -o "$out.tmp" \
        "https://copernicus-dem-30m.s3.eu-central-1.amazonaws.com/$tile/$tile.tif"
      mv "$out.tmp" "$out"
    fi
  done
done

echo "Downloading GBIF occurrences..."
uv run scripts/gbif_download.py data/raw/gbif

du -sh data/raw/*
