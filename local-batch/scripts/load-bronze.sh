#!/usr/bin/env bash
# Uploads the raw files in data/raw to the bronze layer in RustFS, partitioned by the date and
# hour they were downloaded, and registers the external tables and their partitions.
set -euo pipefail
cd "$(dirname "$0")/.."

partition() {
  echo "date=$(date -u -r "$1" +%Y-%m-%d)/hour=$(date -u -r "$1" +%H)"
}
gbif=$(partition data/raw/gbif/download.json)
overture=$(partition data/raw/overture/division_area.parquet)
dem=$(partition data/raw/dem)

# The metastore waits for the bucket to be created, and it is needed later to register the tables
docker compose up -d --wait metastore

echo "Uploading raw files to s3://spatial-lakehouse/bronze..."
docker compose run --rm -T -v ./data/raw:/raw:ro --entrypoint sh rustfs-init -c "
  set -e
  rc alias set rustfs \"\$S3_ENDPOINT\" \"\$S3_ACCESS_KEY\" \"\$S3_SECRET_KEY\" > /dev/null
  rc mirror --quiet /raw/gbif/occurrence.parquet rustfs/spatial-lakehouse/bronze/gbif/occurrence/$gbif
  rc put --quiet /raw/overture/division_area.parquet rustfs/spatial-lakehouse/bronze/overture/division_area/$overture/division_area.parquet
  rc mirror --quiet /raw/dem rustfs/spatial-lakehouse/bronze/copernicus/dem/$dem
"

echo "Registering tables..."
docker compose run --rm spark spark-sql -f sql/lakehouse.sql
