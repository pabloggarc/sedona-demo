# The arrival of the geospatial lakehouse

<table align="center">
  <tr>
    <td align="center" width="110"><img src="docs/images/spark.png" height="56" alt="Apache Spark"><br><sub>Spark</sub></td>
    <td align="center" width="110"><img src="docs/images/sedona.png" height="56" alt="Apache Sedona"><br><sub>Sedona</sub></td>
    <td align="center" width="110"><img src="docs/images/iceberg.png" height="56" alt="Apache Iceberg"><br><sub>Iceberg</sub></td>
    <td align="center" width="110"><img src="docs/images/hive.svg" height="56" alt="Apache Hive"><br><sub>Hive</sub></td>
    <td align="center" width="110"><img src="docs/images/trino.png" height="56" alt="Trino"><br><sub>Trino</sub></td>
    <td align="center" width="110"><img src="docs/images/s3.svg" height="56" alt="Amazon S3"><br><sub>S3 / RustFS</sub></td>
  </tr>
</table>

A demo of what [Apache Sedona](https://sedona.apache.org), the spatial extension of Apache Spark,
brings to a lakehouse with Apache Iceberg tables, in two parts. The first one runs on a laptop with
Docker Compose, on a slice of the data, so that every step can be followed and every job exists in
Python, R and Scala. The second one runs the same spatial join on AWS over the whole planet, about
3,200 million observations of species against 555,000 municipalities, a size no single machine can
handle.

## The problem

Spark and the open table formats were built for rows of numbers, strings and dates. They have no
idea what a polygon is, and the most common spatial question, which polygon each point falls in,
turns into a join whose condition is a geometric test instead of two keys being equal. Without
help, Spark solves it by checking every point against every polygon, which grows with the
product of both sides. The usual spatial tools, such as PostGIS or `sf` in R, do understand
geometries, but they run on a single machine and need the data to fit in it.

## How Sedona solves it

Sedona adds geometry and raster types to Spark, more than 200 `ST_` and `RS_` functions with the
same names as in PostGIS, and readers and writers for GeoParquet and GeoTIFF. More importantly,
it plugs into Spark's optimizer. When a join condition is spatial, Sedona plans a spatial join:
it builds an R-tree index over one side and only checks each point against the polygons whose
bounding box contains it. When the polygons fit in memory, the index is broadcast to every
executor. When they do not, both sides are partitioned by location first, and each partition
gets its own index.

## What the demo does

It counts the species recorded by [GBIF](https://www.gbif.org) in every municipality, using the
observations as points and the municipalities of [Overture Maps](https://overturemaps.org) as
polygons.

**The local part** does it for Castilla-La Mancha, in Spain: 3.6 million observations and 926
municipalities. It also relates the species of each municipality to how rugged its terrain is,
with the [Copernicus DEM](https://registry.opendata.aws/copernicus-dem/) as a raster with the
elevation of the ground every 30 meters. The data goes through three layers. Bronze holds the
files as they were downloaded, as external tables. Silver turns them into Iceberg tables, cleaned
and with the same conventions for the spatial part. Gold holds the results of the spatial joins.
Trino can query every layer with SQL, and has its own spatial functions.

**The cloud part** runs the spatial join for the whole planet on Amazon EMR, reading GBIF and
Overture straight from their public buckets on S3. There the municipalities no longer fit in the
memory of a single machine, so Sedona has to partition both sides by location across the cluster.
It is [work in progress](#the-cloud-part).

The setup favors being easy to read over being realistic, and it is not meant for production.
[This is not a production setup](#this-is-not-a-production-setup) explains what would change.

## Contents

- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Walkthrough](#walkthrough)
- [The cloud part](#the-cloud-part)
- [The data](#the-data)
- [Versions, and why these ones](#versions-and-why-these-ones)
- [Known limitations](#known-limitations)
- [This is not a production setup](#this-is-not-a-production-setup)
- [Repository layout](#repository-layout)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [References](#references)
- [License](#license)

## Architecture

The local part runs entirely on Docker Compose, with RustFS standing in for S3.

```
  ┌──────────────────────┐   download-data.sh   ┌──────────────────────────────────────────┐
  │ GBIF download API    │ ───────────────────▶ │ data/raw                                 │
  │ Overture Maps (S3)   │                      │ Parquet, GeoParquet, GeoTIFF             │
  │ Copernicus DEM (S3)  │                      └────────────────────┬─────────────────────┘
  └──────────────────────┘                                           │ load-bronze.sh
                                                                     ▼
  ┌──────────────────────────────────────────────────────────────────────────────────────┐
  │ RustFS (S3)  s3://spatial-lakehouse                                                  │
  │                                                                                      │
  │  bronze/  raw files, partitioned by download date and hour      external tables      │
  │     │                                                                                │
  │     │  bronze_to_silver   (MERGE by key)                                             │
  │     ▼                                                                                │
  │  silver/  occurrences, municipalities                           Iceberg              │
  │     │                                                                                │
  │     │  silver_gold_municip   (points in polygons)                                    │
  │     │  silver_gold_terrain   (raster zonal statistics, reads the DEM from bronze)    │
  │     ▼                                                                                │
  │  gold/    obs_municipality, terrain_municipality                Iceberg              │
  └──────────────────────────────────────────────────────────────────────────────────────┘
          ▲                          ▲                                    ▲
          │ reads and writes         │ table → location / metadata.json   │ reads
  ┌───────┴────────────────┐   ┌─────┴──────────────────┐   ┌─────────────┴──────────┐
  │ Spark 4.1 + Sedona 1.9 │──▶│ Hive Metastore 3.1     │◀──│ Trino 483              │
  │ Python, R or Scala     │   │ one catalog for all    │   │ SQL and its own ST_    │
  └────────────────────────┘   └────────────────────────┘   └────────────────────────┘
```

| Component | Role |
|---|---|
| **Spark with Sedona** | Runs the jobs. Sedona adds the spatial types and functions and plans the spatial joins. |
| **Iceberg** | The table format of silver and gold: transactions, MERGE, snapshots and file-level statistics. |
| **Hive Metastore** | The catalog for every layer, the external tables of bronze and the Iceberg tables, the same way Glue would be on AWS. |
| **RustFS** | S3-compatible storage for the whole lakehouse. |
| **Trino** | Queries the tables with SQL from outside Spark. Amazon Athena is built on it. |

### How geometries are stored

Silver and gold store every shape in a `geometry` column as WKB, the standard binary encoding
for geometries, next to a `bbox` struct with its bounding box (`xmin`, `ymin`, `xmax`, `ymax`),
and the rows are sorted by geohash so that nearby shapes end up in the same files.

```
  one file of silver.occurrences
  ┌──────────┬───────────┬──────────────────┬─────────────────────────────────────┐
  │ gbif_id  │ species   │ geometry (WKB)   │ bbox                                │
  ├──────────┼───────────┼──────────────────┼─────────────────────────────────────┤
  │ ...      │ ...       │ 0101000000...    │ {-3.71, 39.85, -3.71, 39.85}        │
  └──────────┴───────────┴──────────────────┴─────────────────────────────────────┘
       Iceberg keeps the min and max of every bbox column per file, so a query that
       filters on bbox skips the files whose rectangle does not overlap its area
```

Iceberg v3 has its own geometry type, but Sedona cannot write it yet and Spark does not push
spatial filters down to Iceberg, as explained in [Known limitations](#known-limitations). The
bounding box in plain numeric columns gives the same file skipping today, in any engine. It is
also what GeoParquet and Overture do.

## Quick start

This starts the local part. You need Docker with Compose v2 and about **12 GB of memory for Docker** to run Spark and Trino at
the same time (Spark alone needs around 8 GB). The first build downloads a few GB of images and
jars. You also need a free [GBIF account](https://www.gbif.org/user/profile), since GBIF prepares
the download of the occurrences on its side.

```bash
git clone <this-repo> && cd sedona-demo/local-batch
cp .env.example .env              # and fill in your GBIF account
./scripts/download-data.sh        # about 1.3 GB, and GBIF takes a while to prepare its file
./scripts/load-bronze.sh          # uploads data/raw to bronze and registers the tables
docker compose run --rm spark spark-submit jobs/python/bronze_to_silver.py
docker compose run --rm spark spark-submit jobs/python/silver_gold_municip.py
docker compose run --rm spark spark-submit jobs/python/silver_gold_terrain.py
```

Each job takes a minute or two. Then query the result with Trino.

```bash
docker compose up -d trino
docker compose exec trino trino --execute "
  SELECT name, species, observations
  FROM iceberg.gold.obs_municipality
  ORDER BY species DESC LIMIT 5"
```

```
 name         | species | observations
--------------+---------+--------------
 Cuenca       |    3286 |        53684
 Alcoba       |    2137 |        18263
 Yeste        |    2059 |         9927
 Riópar       |    2025 |        13728
 Fuencaliente |    2006 |        14191
```

The municipalities of the mountain ranges come first: the Serranía de Cuenca, Cabañeros National
Park, the Sierra de Alcaraz and the Sierra Madrona.

Only the ports that are useful for poking around are published.

| What | Where | Credentials |
|---|---|---|
| Trino web UI | http://localhost:8080 | any user name |
| RustFS console | http://localhost:9001/rustfs/console/ | `rustfsadmin` / `rustfsadmin` |
| Spark UI | http://localhost:4040, while a job or shell started with `--service-ports` runs | none |

The RustFS credentials live in `.env`, next to the GBIF account. The S3 API of RustFS and the
Hive Metastore are only reachable from inside the Docker network.

## Walkthrough

The steps below follow the data of the local part from the sources to the queries. Run them from
`local-batch/`.

### 1. Download the data

[`scripts/download-data.sh`](local-batch/scripts/download-data.sh) downloads a slice of each
source into `data/raw`. Overture and the DEM are cut to the bounding box of Castilla-La Mancha,
and the GBIF occurrences are filtered on GBIF's side by the region they fall in, through the
[GBIF download API](https://techdocs.gbif.org/en/openapi/v1/occurrence#/Occurrence%20downloads).
GBIF takes half an hour or so to prepare the file. The script waits for it, and if it is stopped,
running it again resumes the same download.

```
Downloading Overture division areas...
Downloading Copernicus DEM tiles...
Downloading GBIF occurrences...
requested download 0024396-260928105237408
download 0024396-260928105237408 is running
...
3,635,163 occurrences, doi 10.15468/dl.wjkaxq
```

Every GBIF download gets a DOI, saved in `data/raw/gbif/download.json`, which is how GBIF asks to
cite the data.

### 2. Bronze

[`scripts/load-bronze.sh`](local-batch/scripts/load-bronze.sh) uploads the files to RustFS, one
folder per source and partitioned by the date and hour they were downloaded, as a traditional
bronze layer would. Then [`sql/lakehouse.sql`](local-batch/sql/lakehouse.sql) creates the three
databases and the external tables, and registers their partitions.

```
spatial-lakehouse/bronze/
├── gbif/occurrence/date=2026-10-13/hour=17/            5,972 Parquet files, 404 MB
├── overture/division_area/date=2026-10-13/hour=16/     one GeoParquet file
└── copernicus/dem/date=2026-10-13/hour=16/             24 GeoTIFF tiles
```

Spark infers the partition columns from the folder names, so the SQL file does not declare them.

### 3. Silver

[`bronze_to_silver`](local-batch/jobs/python/bronze_to_silver.py) reads the downloads of the last
year from bronze, keeps the latest version of every record, and merges it into two Iceberg tables
keyed by their id.

- **`silver.occurrences`** has every GBIF record, typed and renamed but not filtered, because
  records of absence or identified only down to genus are still valid data. Deciding what counts
  is up to each analysis in gold.
- **`silver.municipalities`** has the municipalities of Overture with their area on the Earth's
  surface.

Both follow the conventions described in [How geometries are stored](#how-geometries-are-stored).
The tables are created with `WRITE ORDERED BY geohash`, because a MERGE does not keep the order of
the DataFrame it comes from. The 5,972 files of bronze end up as 11 files and 97 MB in silver.

### 4. Gold: observations per municipality

[`silver_gold_municip`](local-batch/jobs/python/silver_gold_municip.py) joins every occurrence to
the municipality that contains it and counts observations and distinct species. It only keeps
records of presence, identified down to species and placed within 1 km. The job prints its plan.

```
+- BroadcastIndexJoin (8)                         ← Sedona's spatial join
   :- SpatialIndex (4)                            ← R-tree over the 925 polygons
   :  +- BatchScan spark_catalog.silver.municipalities (1)
   +- BatchScan spark_catalog.silver.occurrences (5)
```

The join condition is `ST_Contains(polygon, point)`. Sedona sees it, builds an R-tree over the
polygons, broadcasts it, and checks each point only against the polygons around it. The details
of both scans also show the filters pushed down to Iceberg.

It has to be an inner join. Sedona's spatial joins also handle outer joins that keep the side
that is streamed (the points), but not the side that is indexed. A left join to keep the
municipalities without observations falls back to `BroadcastNestedLoopJoin`, which checks every
point against every polygon and took over ten minutes instead of one.

### 5. Gold: terrain per municipality

[`silver_gold_terrain`](local-batch/jobs/python/silver_gold_terrain.py) reads the DEM tiles
straight from bronze, since there is no standard yet for raster data in a lakehouse table. Each
tile covers 1x1 degree with 3,600 x 3,600 pixels, so the job cuts it into pieces of 512 x 512 to
spread the work and avoid going through pixels that no municipality needs. Every piece keeps its
own georeference, the coordinates of its corner and the size of a pixel.

Then it joins each piece to the municipalities it overlaps, with `RS_Intersects`, and computes
zonal statistics, the mean, minimum, maximum and standard deviation of the pixels that fall
inside the outline of each municipality. A municipality usually overlaps several pieces, so its
statistics are put together from the count, sum and variance of each of them. The plan shows the
same `BroadcastIndexJoin`, this time between pieces of a raster and polygons.

The results match reality. The mean elevation of Toledo comes out at 529.6 m (the city is at
529 m), and the highest point, 2,264.9 m in El Cardoso de la Sierra, is the Pico del Lobo
(2,273 m).

### 6. Query the lakehouse with Trino

Trino reads the Iceberg tables and the bronze table of GBIF from the same metastore, through two
catalogs, `iceberg` and `hive`.

```bash
docker compose exec trino trino
```

```sql
SELECT round(corr(o.species, t.elevation_stddev), 3) AS relief,
       round(corr(o.species, t.elevation_mean), 3)   AS height
FROM iceberg.gold.obs_municipality o
JOIN iceberg.gold.terrain_municipality t USING (municipality_id);
```

```
 relief | height
--------+--------
  0.452 |  0.068
```

The number of species follows how rugged a municipality is, not how high it is. Part of that is
the size of the municipalities, though, since the mountainous ones are also the largest.

Trino has its own spatial functions, so it can work with the WKB geometries directly. The species
seen within 5 km of the cathedral of Toledo:

```sql
SELECT count(DISTINCT species)
FROM iceberg.silver.occurrences
WHERE occurrence_status = 'PRESENT'
  AND bbox.xmin BETWEEN -4.08 AND -3.96
  AND bbox.ymin BETWEEN 39.81 AND 39.91
  AND ST_Distance(
    to_spherical_geography(ST_GeomFromBinary(geometry)),
    to_spherical_geography(ST_Point(-4.0244, 39.8569))
  ) < 5000;
```

The filter on `bbox` is what skips files. With it, `EXPLAIN ANALYZE` shows 3 files read instead of
10, and less than a third of the CPU time.

### 7. The same jobs in R and Scala

Every job also exists in [`jobs/r`](local-batch/jobs/r), written with sparklyr and dplyr, and in
[`jobs/scala`](local-batch/jobs/scala). All three versions build the same plan and write the
same tables, so they can be mixed.

```bash
docker compose run --rm spark Rscript jobs/r/silver_gold_municip.R
docker compose run --rm -T spark spark-shell \
  -i jobs/scala/lakehouse.scala -i jobs/scala/silver_gold_municip.scala < /dev/null
```

The spatial join in R is a regular dplyr join, and dbplyr passes the `ST_` functions through to
Spark SQL as they are.

```r
municipalities |>
  inner_join(occurrences, sql_on = "ST_Contains(LHS.polygon, RHS.point)") |>
  group_by(municipality_id, name, area_km2, geometry, bbox, geohash) |>
  summarise(observations = n(), species = n_distinct(species), .groups = "drop")
```

To work step by step, open a shell instead. The jobs are written top to bottom, so they can be
pasted into it.

```bash
docker compose run --rm --service-ports spark                       # PySpark
docker compose run --rm --service-ports spark R                     # then source("jobs/r/lakehouse.R")
docker compose run --rm --service-ports spark spark-shell -i jobs/scala/lakehouse.scala
```

## The cloud part

The local part proves the pipeline on 3.6 million points. The cloud part takes the spatial join to
the size where it stops being possible on one machine.

| | Local | Cloud |
|---|---|---|
| Observations | 3.6 million (Castilla-La Mancha) | about 3,200 million (the whole GBIF snapshot on S3, 288 GB) |
| Municipalities | 926 | 555,000, in 229 countries |
| Spatial join | `BroadcastIndexJoin`, the polygons fit in memory | `RangeJoin`, both sides partitioned by location |
| Compute | Spark in local mode on a laptop | Amazon EMR |
| Storage and catalog | RustFS and a Hive Metastore | S3 and the Glue Data Catalog, with external tables over the public buckets |

Checking every point against every municipality would mean about 1.75 × 10¹⁵ point-in-polygon
tests. With Sedona's spatial index, each point is only checked against the few polygons around
it. The plan is to run it on a cluster of around 20 nodes with Spot instances, which should take
well under two hours and cost in the order of ten dollars, and to launch it from R as well.

This part is being built now, and it will live in a `cloud-batch/` folder next to `local-batch/`.

## The data

| Source | What | Slice used | Format | License |
|---|---|---|---|---|
| [GBIF](https://www.gbif.org) | Occurrences of species, from museums, herbaria and citizen science apps | 3,635,163 records in Castilla-La Mancha | Parquet | CC0, CC BY or CC BY-NC, depending on the dataset |
| [Overture Maps](https://overturemaps.org), divisions | Administrative areas, from countries to neighborhoods | 4,557 areas in the bounding box, 926 municipalities of Castilla-La Mancha | GeoParquet | ODbL |
| [Copernicus DEM](https://registry.opendata.aws/copernicus-dem/) GLO-30 | Elevation of the ground every 30 m | 24 tiles of 1x1 degree | Cloud Optimized GeoTIFF | Free for any use, with attribution |

Overture and the DEM are read from their public buckets on AWS, and the download needs no AWS
account. If you show results built on the GBIF records, cite the DOI of your download, as asked in
the [GBIF citation guidelines](https://www.gbif.org/citation-guidelines).

The data has the biases of citizen science. Two thirds of the records in Castilla-La Mancha are of
birds, 40% come from eBird alone, and the Madrid region has more observations than the whole of
Castilla-La Mancha, which is ten times larger. The maps built from it show where people look as much as where species
live.

To use another region, change the bounding box and the GADM id of the region at the top of
`download-data.sh`, and the region code in the gold jobs.

## Versions, and why these ones

| Component | Version | Image |
|---|---|---|
| Apache Spark | 4.1.3 | `sedona-demo/spark`, built from `eclipse-temurin:17-jre-noble` and PySpark from PyPI |
| Apache Sedona | 1.9.1 | jars from Maven Central, `apache-sedona` from PyPI |
| Apache Iceberg | 1.12.0 | Spark runtime jar from Maven Central |
| Hive Metastore | 3.1.3 (Derby) | `apache/hive:3.1.3`, amd64 only |
| Trino | 483 | `trinodb/trino:483` |
| RustFS | 1.0.1 | `rustfs/rustfs:1.0.1` |
| R packages | sparklyr 1.9.6, dplyr, dbplyr 2.6.0 | binaries from Posit Package Manager, snapshot of 2026-10-15 |

**Spark 4.1.** It is the only Spark version Iceberg supports geometry types with, according to the
[Iceberg 1.12.0 release notes](https://iceberg.apache.org/blog/apache-iceberg-1.12.0-release/),
and the Spark version of the long-term support release of Amazon EMR.

**A custom Spark image.** The official `apache/spark` image ships Python 3.10, too old for
current geopandas and pyarrow, and `apache/sedona` is still on Spark 4.0 (its `latest` tag is a
2.0.0 snapshot, also on Spark 4.0). Spark installed from PyPI on a plain Java image gives Spark
4.1 with Python 3.12, and its `spark-shell` already includes the Scala compiler, so the Scala jobs
need nothing else. The image is around 3 GB, most of it the AWS SDK that the S3A connector needs.

**Hive Metastore 3.1.3, not 4.x.** Spark talks to the metastore with its built-in Hive 2.3
client, which works with HMS 3.x and not with 4.x. There is no arm64 image of Hive 3.1.3, so on
Apple Silicon it runs emulated.

**R without the apache.sedona package.** The 1.9.1 release on CRAN stops with
`Unsupported Spark version` on Spark 4. It is fixed in
[apache/sedona#3340](https://github.com/apache/sedona/pull/3340), but not released yet. It is not
needed anyway: `spark-defaults.conf` registers Sedona in every session, and dbplyr passes the
spatial functions straight to Spark SQL.

**Trino 483, not 484.** 484 came out the day Trino was added to the demo, so the previous release
felt like the safer choice.

## Known limitations

These are limits of the versions above, not of the demo setup. I checked each of them against the
code or reproduced it.

- **No native geometry columns in Iceberg.** Sedona 1.9 represents geometries with its own Spark
  type, and Iceberg's Spark connector only maps Spark 4.1's native `GEOMETRY` type, so writing a
  Sedona geometry fails with `User-defined types are not supported`
  ([SEDONA-744](https://issues.apache.org/jira/browse/SEDONA-744)). Going through Spark's native
  type is possible with internal APIs only, because Sedona replaces Spark's `ST_` functions with
  its own at startup. Sedona 2.0 plans to move to Spark 4.2's native types
  ([apache/sedona#3124](https://github.com/apache/sedona/issues/3124)).
- **No spatial filter pushdown into Iceberg.** Even with a native geometry column, Spark does not
  record per-file bounds for it and does not push spatial filters to Iceberg. The Iceberg 1.12
  release notes say it plainly: "there are no spatial predicates yet, so filters are expressed
  against non-geospatial columns". Hence the `bbox` columns.
- **No raster functions in Sedona's DataFrame API**, in Python or Scala. The `RS_` functions are
  only registered in SQL, so the jobs call them with `call_function`. R does not have this problem,
  since dbplyr translates everything to SQL.
- **Spatial joins must be inner joins**, or outer joins that keep the streamed side. See
  [step 4](#4-gold-observations-per-municipality).
- **MERGE from a temporary view over Iceberg tables fails** in Spark 4.1 with
  `No plan for TableReference`. It only affects R, where the source of the MERGE is a view, and
  `dplyr::compute()` works around it by caching the source first.
- **Trino cannot read `bronze.overture_division_area`.** It is registered as GeoParquet, a format
  only Spark with Sedona understands. The GBIF table, in plain Parquet, it reads fine.

## This is not a production setup

Everything here is tuned so that a laptop can run it and a person can follow it.

- **A slice of the data.** One region, so that every job runs in a minute or two. The whole
  planet is what [the cloud part](#the-cloud-part) is for.
- **Spark in local mode.** Every job runs in a single JVM on the cores of the laptop.
- **A lookback window of one year.** `bronze_to_silver` only reads the downloads of the last 365
  days. A real pipeline would read only what arrived since the last run.
- **Throwaway state.** The Hive Metastore keeps its catalog in an embedded Derby database inside
  its container. `docker compose down` forgets the tables while their files stay in RustFS, and
  `docker compose down -v` removes the files too.
- **No security.** There is no TLS and no authentication between the services, and the RustFS
  credentials are the same for everything.

## Repository layout

Each folder of `local-batch/` holds everything its containers need. `scripts/` holds what you run
from your own machine.

```
.
├── README.md
├── docs/images/              logos used in this README
├── cloud-batch/              the cloud part, coming next
└── local-batch/
    ├── docker-compose.yml    services, ports, volumes, dependencies
    ├── .env.example          GBIF account and RustFS credentials, copied to .env
    ├── rustfs/               S3 storage settings and the script that creates the bucket
    ├── metastore/            Hive Metastore image with S3A, and its core-site.xml
    ├── spark/                Spark image (Sedona, Iceberg, S3A, R), spark-defaults.conf, logging
    ├── trino/                Trino catalogs for Iceberg and Hive
    ├── sql/lakehouse.sql     databases and external tables of bronze
    ├── scripts/
    │   ├── download-data.sh  downloads the slice of every source into data/raw
    │   ├── gbif_download.py  requests, waits for and extracts the GBIF download
    │   └── load-bronze.sh    uploads data/raw to bronze and registers the tables
    ├── jobs/
    │   ├── python/           bronze_to_silver, silver_gold_municip, silver_gold_terrain, lakehouse
    │   ├── r/                the same jobs with sparklyr and dplyr
    │   └── scala/            the same jobs as spark-shell scripts
    └── data/raw/             the downloaded files, not committed
```

These are the settings you are most likely to play with.

| Setting | Where | Default |
|---|---|---|
| Region of the data | `BBOX` and `GADM_GID` in [`download-data.sh`](local-batch/scripts/download-data.sh), and the region code in the gold jobs | Castilla-La Mancha |
| Lookback window of silver | `LOOKBACK_DAYS` in `bronze_to_silver` | 365 days |
| Size of the raster pieces | `RS_TileExplode` in `silver_gold_terrain` | 512 x 512 pixels |
| Spark memory | `spark.driver.memory` in [`spark-defaults.conf`](local-batch/spark/spark-defaults.conf), and `connect()` in [`jobs/r/lakehouse.R`](local-batch/jobs/r/lakehouse.R) | 6 GB |

## Troubleshooting

**A port is already in use.** The demo needs 4040, 8080 and 9001 on the host.

**A job dies with `OutOfMemoryError`.** Give Docker more memory, or stop Trino while the job runs
with `docker compose stop trino`. From R, keep the driver memory in `connect()`, because in local
mode sparklyr passes its own `--driver-memory 2g`, which wins over `spark-defaults.conf`.

**The GBIF download is not in your GBIF account yet.** The script downloads Overture and the DEM
first, and only then asks GBIF for the occurrences.

**Spark fails to infer the partitions of bronze.** This happens if the bucket holds folders with
two different partition schemes, for example after changing them. Start from scratch with
`docker compose down -v` and run `load-bronze.sh` again.

**The metastore is slow to start on a Mac.** It runs under amd64 emulation. Make sure Rosetta is
enabled in Docker Desktop (Settings → General → "Use Rosetta for x86_64/amd64 emulation").

## Stopping

```bash
docker compose stop       # pause, keeping the state (resume with: docker compose start)
docker compose down -v    # remove everything, including the lakehouse in RustFS
```

## Contributing

Pull requests are welcome, whether they fix something, make an explanation clearer, move the demo
to newer versions or add another way of looking at the data. For bigger changes, opening an issue first to talk it
through saves everyone time.

## References

- [Apache Sedona documentation](https://sedona.apache.org/latest/)
- [Sedona spatial join and its parameters](https://sedona.apache.org/latest/api/sql/Parameter/)
- [Sedona DataFrame API](https://sedona.apache.org/latest/api/sql/DataFrameAPI/)
- [Apache Iceberg 1.12.0 release notes](https://iceberg.apache.org/blog/apache-iceberg-1.12.0-release/)
- [GeoParquet specification](https://geoparquet.org/)
- [Trino geospatial functions](https://trino.io/docs/current/functions/geospatial.html)
- [sparklyr](https://spark.posit.co/)
- [GBIF on the AWS Open Data Registry](https://registry.opendata.aws/gbif/)
- [Overture Maps divisions](https://docs.overturemaps.org/guides/divisions/)
- [Copernicus DEM on AWS](https://registry.opendata.aws/copernicus-dem/)
- [RustFS](https://github.com/rustfs/rustfs)

## License

This project is licensed under the [Apache License 2.0](LICENSE).

---

<sub>Apache Spark, Apache Sedona, Apache Iceberg, Apache Hive and their logos are trademarks of the
[Apache Software Foundation](https://www.apache.org/foundation/marks/). Trino and its logo belong to
the Trino Software Foundation, and Amazon S3 is a trademark of Amazon.com, Inc. They are used here
only to identify each project. The demo uses RustFS, an S3-compatible store, not Amazon S3.</sub>
