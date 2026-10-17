CREATE DATABASE IF NOT EXISTS bronze LOCATION 's3a://spatial-lakehouse/bronze';
CREATE DATABASE IF NOT EXISTS silver LOCATION 's3a://spatial-lakehouse/silver';
CREATE DATABASE IF NOT EXISTS gold LOCATION 's3a://spatial-lakehouse/gold';

CREATE TABLE IF NOT EXISTS bronze.gbif_occurrence
USING parquet
LOCATION 's3a://spatial-lakehouse/bronze/gbif/occurrence';

CREATE TABLE IF NOT EXISTS bronze.overture_division_area
USING geoparquet
LOCATION 's3a://spatial-lakehouse/bronze/overture/division_area';

MSCK REPAIR TABLE bronze.gbif_occurrence;
MSCK REPAIR TABLE bronze.overture_division_area;
