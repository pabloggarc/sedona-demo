# The arrival of the geospatial lakehouse

<table align="center">
  <tr>
    <td align="center" width="110"><img src="docs/images/spark.png" height="56" alt="Apache Spark"><br><sub>Spark</sub></td>
    <td align="center" width="110"><img src="docs/images/sedona.png" height="56" alt="Apache Sedona"><br><sub>Sedona</sub></td>
    <td align="center" width="110"><img src="docs/images/iceberg.png" height="56" alt="Apache Iceberg"><br><sub>Iceberg</sub></td>
    <td align="center" width="110"><img src="docs/images/hive.svg" height="56" alt="Apache Hive"><br><sub>Hive Metastore</sub></td>
    <td align="center" width="110"><img src="docs/images/trino.png" height="56" alt="Trino"><br><sub>Trino</sub></td>
    <td align="center" width="110"><img src="docs/images/rustfs.png" height="56" alt="RustFS"><br><sub>RustFS</sub></td>
  </tr>
</table>
<table align="center">
  <tr>
    <td align="center" width="110"><img src="docs/images/emr.svg" height="56" alt="Amazon EMR"><br><sub>EMR</sub></td>
    <td align="center" width="110"><img src="docs/images/glue.svg" height="56" alt="AWS Glue"><br><sub>Glue Data Catalog</sub></td>
    <td align="center" width="110"><img src="docs/images/athena.svg" height="56" alt="Amazon Athena"><br><sub>Athena</sub></td>
    <td align="center" width="110"><img src="docs/images/s3.svg" height="56" alt="Amazon S3"><br><sub>S3</sub></td>
  </tr>
</table>

A demo of what [Apache Sedona](https://sedona.apache.org), the spatial extension of Apache Spark,
brings to a lakehouse with Apache Iceberg tables. It comes in two parts. The first one runs on a
laptop with Docker Compose, on one region, so that every step can be followed, and every job exists
in Python, R and Scala. The second one takes the same spatial join to Amazon EMR and runs it for the
whole planet, straight from the public buckets of the data on S3: the 3,900 million records of GBIF
against 555,000 localities, in under five minutes.

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

**The cloud part** runs the spatial join on Amazon EMR, with the infrastructure in Terraform and
the job in Scala. It reads GBIF and Overture where they are published, and writes the result as an
Iceberg table that Athena can query once the cluster is gone. At that size the polygons no longer
fit in the memory of every executor, so Sedona partitions both sides by location across the
cluster. A country takes about a minute, and the planet four and a half.

The local part favors being easy to read over being realistic, and it is not meant for production.
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

The cloud part keeps the same idea on AWS, with S3 and the Glue Data Catalog in the place of RustFS
and the metastore. It skips the copy into bronze: its bronze tables are external tables over the
public buckets of GBIF and Overture.

```
  ┌─────────────────────────────────┐        ┌─────────────────────────────────┐
  │ s3://gbif-open-data-us-east-1   │        │ s3://overturemaps-us-west-2     │
  │ GBIF snapshot, 303 GB Parquet   │        │ Overture divisions, GeoParquet  │
  └────────────────┬────────────────┘        └────────────────┬────────────────┘
                   │      bronze: external tables in Glue      │
                   ▼                                           ▼
  ┌──────────────────────────────────────────────────────────────────────────────┐
  │ Amazon EMR, in a VPC with one public subnet                                  │
  │                                                                              │
  │   primary   m7g.2xlarge, on demand     YARN, HDFS and the Spark driver       │
  │   core      1 x c7g.8xlarge, on demand ┐ Spark executors, 192 cores, one     │
  │   task      5 x c7g.8xlarge, spot      ┘ task per physical core              │
  └───────────────────────────────────────┬──────────────────────────────────────┘
                                          │ obs_municipality (Iceberg)
                                          ▼
  ┌──────────────────────────────────────────────────────────────────────────────┐
  │ s3://<account>-spatial-lakehouse                                             │
  │                                                                              │
  │   data/gold/    the results, as Iceberg tables in the Glue Data Catalog      │
  │   code/jars/    the jar of the Scala job                                     │
  │   temp/         Athena results, EMR logs and Spark event logs, which expire  │
  └───────────────────────────────────────┬──────────────────────────────────────┘
                                          │ SQL
                              Amazon Athena (workgroup geospatial-lakehouse)
```

| Component | Role |
|---|---|
| **Amazon EMR** | Runs Spark with Sedona on a cluster that lives only for the run. |
| **Glue Data Catalog** | The catalog of the three layers, the same role the Hive Metastore has in the local part. |
| **S3** | The lakehouse bucket, plus the public buckets the data is read from. |
| **Athena** | Queries the tables with SQL once the cluster is gone, as Trino does locally. |
| **Terraform** | Creates all of the above, with one file per service in `cloud/terraform`. |

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
git clone <this-repo> && cd sedona-demo/local
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
| Spark History Server | http://localhost:18080, with the runs saved from EMR | none |

The RustFS credentials live in `.env`, next to the GBIF account. The S3 API of RustFS and the
Hive Metastore are only reachable from inside the Docker network.

### On AWS

The cloud part needs an AWS account, Terraform 1.9 or later, the AWS CLI and Docker, which builds
the jar so you do not need a JDK or sbt. It runs in `us-east-1`, where GBIF keeps its copy. The
cluster costs about 5.5 dollars an hour while it is up, and the planet takes less than five minutes
of it.

```bash
cd sedona-demo/cloud
./scripts/build.sh                                      # the jar of the Scala job
cd terraform
terraform init
terraform apply                                         # about 10 minutes, mostly the cluster
cd ..
./scripts/submit.sh sedonademo.ObsMunicipality ES       # any country code, or world
./scripts/save-run.sh                                   # keeps the Spark event logs of the run
cd terraform && terraform destroy
```

[The cloud part](#the-cloud-part) goes through each step.

## Walkthrough

The steps below follow the data of the local part from the sources to the queries. Run them from
`local/`.

### 1. Download the data

[`scripts/download-data.sh`](local/scripts/download-data.sh) downloads a slice of each
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

[`scripts/load-bronze.sh`](local/scripts/load-bronze.sh) uploads the files to RustFS, one
folder per source and partitioned by the date and hour they were downloaded, as a traditional
bronze layer would. Then [`sql/lakehouse.sql`](local/sql/lakehouse.sql) creates the three
databases and the external tables, and registers their partitions.

```
spatial-lakehouse/bronze/
├── gbif/occurrence/date=2026-10-13/hour=17/            5,972 Parquet files, 404 MB
├── overture/division_area/date=2026-10-13/hour=16/     one GeoParquet file
└── copernicus/dem/date=2026-10-13/hour=16/             24 GeoTIFF tiles
```

Spark infers the partition columns from the folder names, so the SQL file does not declare them.

### 3. Silver

[`bronze_to_silver`](local/jobs/python/bronze_to_silver.py) reads the downloads of the last
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

[`silver_gold_municip`](local/jobs/python/silver_gold_municip.py) joins every occurrence to
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

[`silver_gold_terrain`](local/jobs/python/silver_gold_terrain.py) reads the DEM tiles
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

Every job also exists in [`jobs/r`](local/jobs/r), written with sparklyr and dplyr, and in
[`jobs/scala`](local/jobs/scala). All three versions build the same plan and write the
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
the size where it stops being possible on one machine: every record of the monthly GBIF snapshot
against every locality of Overture. It lives in `cloud/`, and only covers the observations per
municipality, which is the join that grows with the data.

| | Local | Cloud |
|---|---|---|
| Observations | 3.6 million (Castilla-La Mancha) | 3,913 million records in the GBIF snapshot on S3 (303 GB) |
| Municipalities | 926 | 554,885 localities |
| Spatial join | `BroadcastIndexJoin`, the polygons fit in memory | `RangeJoin`, both sides partitioned by location |
| Compute | Spark in local mode on a laptop | Amazon EMR, 192 Graviton cores |
| Storage and catalog | RustFS and a Hive Metastore | S3 and the Glue Data Catalog, with external tables over the public buckets |
| Language | Python, R and Scala | Scala, as a jar submitted to EMR |

Checking every record against every locality would mean about

$$3.9 \times 10^9 \times 5.5 \times 10^5 \approx 2.2 \times 10^{15}$$

point-in-polygon tests. With Sedona's partitioning and index, each point is only checked against
the one or two pieces of polygon around it.

### Running it

Run the steps from `cloud/`, with credentials for your AWS account in the environment.

[`scripts/build.sh`](cloud/scripts/build.sh) compiles the sbt project in [`jobs/`](cloud/jobs)
inside a Docker image with Java and sbt. Spark and Sedona are marked as provided, since the cluster
brings them, so the jar only holds the classes of the job.

`terraform apply` creates the whole stack, with one file per service in
[`terraform/`](cloud/terraform). There is a VPC with one public subnet and a gateway endpoint for
S3, the lakehouse bucket split into `data/`, `code/` and `temp/` (whatever is under `temp/` expires
after a few weeks), the three databases in Glue with the bronze tables pointing at the public
buckets, an Athena workgroup that refuses queries over 10 GB, and two IAM roles, one for the EMR
service and one for the nodes, which can only touch the lakehouse bucket, the public buckets and
Glue. Last comes the cluster, whose task nodes are spot instances capped at their on-demand price,
which Terraform reads from the AWS Pricing API. It terminates itself after 30 minutes without work.

[`scripts/submit.sh`](cloud/scripts/submit.sh) uploads the jar and adds a step to the cluster that
runs `spark-submit`. The only argument of the job is the scope, a country code such as `ES` or
`world`, and the result replaces the rows of that scope in `gold.obs_municipality`, so a country can
be run again without touching the others. The driver runs on the primary node, so its output ends
up in the log of the step, under `temp/logs/<cluster>/steps/`. The results can then be queried from
Athena, in the `geospatial-lakehouse` workgroup:

```sql
SELECT name, country, observations, species
FROM gold.obs_municipality
WHERE scope = 'world'
ORDER BY species DESC
LIMIT 5;
```

Terminating the cluster from the console stops the costs and leaves the bucket, the tables and the
logs where they are, and the next `terraform apply` creates a new one. `terraform destroy` removes
the bucket as well, so run [`scripts/save-run.sh`](cloud/scripts/save-run.sh) before it if you want
to look at the run later.

### How the job works

[`ObsMunicipality.scala`](cloud/jobs/src/main/scala/sedonademo/ObsMunicipality.scala) does the
same as `silver_gold_municip` in the local part, but straight from the bronze tables, and it
needed three ideas to get there at the size of the planet.

**The observations are grouped first.** GBIF has a lot of records in exactly the same place:
hotspots of eBird visited every day, coordinates rounded to the center of a region, the same point
used for a whole collection. In a sample of 2% of the snapshot, 1,604 coordinates had 2,000 records
or more, and the busiest one had 134,927, which would be close to seven million in the whole
snapshot. That is a problem for Sedona, as explained below, so the job counts the observations by
coordinate and species before the join, which leaves 799 million rows, and a coordinate is never
repeated more times than it has species.

**The grid is drawn from the points.** A `RangeJoin` splits the map into cells, drawn from a sample
of one side of the join so that every cell gets about the same number of its elements. Every cell
then builds an R-tree over the other side and checks its points against it. The job keeps the
polygons on the left, which is the side that gets indexed, and sets
`spark.sedona.join.spatitionside` to `right`, so that the cells follow where the observations are.
Left alone, Sedona would make as many cells as partitions the points arrive in, which has nothing to
do with how many points there are, so the job counts the rows and asks for one cell every 20,000.

**The polygons are cut into pieces.** To check whether a point is inside a polygon, the `RangeJoin`
uses the plain `contains` of JTS, which walks the whole outline. Some coastal localities have tens
of thousands of vertices, 77,866 in the case of Alt Empordà. `ST_SubDivideExplode` cuts every
polygon in two, again and again, along a straight line through the middle of its longer side, until
no piece has more than 256 vertices. Every piece keeps the id of its locality. Cutting a big polygon
is expensive, and the few tasks that read Overture would have to cut all the polygons in their part
of the files, so the polygons are spread over 1,000 partitions first.

Only the id of the locality goes through the spatial join. The name, the country and the outline
come back afterwards with a plain join, instead of being dragged through the partitioning. The job
only exists in Scala, to keep the cloud part simple: a single jar that EMR runs as a step, with
nothing to install on the cluster. It could be written in Python or R like the jobs of the local
part, by installing their packages on the nodes when the cluster starts.

### The whole planet

| | |
|---|---|
| GBIF | 10,448 Parquet files, 3,913 million records |
| Read from S3 | 93 GB, since only 6 of the 50 columns of GBIF are needed |
| Into the spatial join | 799 million rows of coordinate and species |
| Overture | 1.07 million areas, 554,885 of them localities, cut into 1.65 million pieces |
| Cells of the join | 57,346 |
| Result | 368,576 localities in 222 countries and territories, with 1,868 million observations |

| Step | Time |
|---|---|
| Read GBIF and group the observations | 23 s |
| Count the rows to size the grid | 12 s |
| Read Overture | 12 s |
| Sample the points and draw the grid | 14 s |
| Send points and pieces to their cells | 30 s |
| Spatial join | 2 min 30 s |
| Aggregate and write to Iceberg | 13 s |
| **Total** | **4 min 16 s**, and 4 min 36 s with the start of the application |

The tasks of the spatial join took 0.3 seconds in the median and 3.1 seconds the slowest, so no
cell held the rest back. Altogether the tasks added up to 10 hours of work on the 192 cores, 6.8 of
them on the CPU and the rest waiting for S3 and the shuffles. The cluster costs about 5.5 dollars an
hour, so the run itself was worth some 40 cents.

What comes out says as much about who records as about where species live. These are the
localities with the most species:

```
 name       | country | species | observations
------------+---------+---------+--------------
 Sydney     | AU      |   29032 |      8152842
 Brisbane   | AU      |   19760 |      7763015
 Melbourne  | AU      |   18056 |     11563815
 Coral Sea  | AU      |   16506 |       852048
 Helsinki   | FI      |   14677 |      1339288
 New York   | US      |   14675 |     14869539
```

Part of it is the size of the localities, since Sydney covers 4,800 km² in Overture against 1,217
for New York, and part how much people record in each country.

The first try at the planet, without grouping the observations, never got past the grid. Sedona
brought 30 million points to the driver to draw it, and the driver was still alone with them after
nine minutes, while the executors sat idle. As far as I can tell from its code and from the data,
this is why: its KDB-tree splits a cell in two when it gets too full, and a cell full of identical
points cannot be split, so Sedona tries again, sorting the whole cell, with every new point that
lands in it. With the observations grouped, the sample was
8 million points and the grid was ready in 8 seconds.

Spain, on the same cluster, takes 39 seconds of computing out of 65. The Parquet reader skips the
groups of rows that cannot hold the country, so it reads 3,026 million records instead of 3,913.
They become 26.7 million rows of coordinate and species, and 8,655 localities are cut into 30,877
pieces. The spatial join takes 8 seconds over 1,924 cells, and 8,530 localities come out with 108.9
million observations.

### Looking at a run afterwards

The Spark UI dies with the cluster, but it can be rebuilt from the event logs that Spark writes as
it goes. `save-run.sh` copies them from the HDFS of the cluster to the bucket and then to
`runs/events/`, and the logs of EMR to `runs/logs/<cluster>/`. The `spark-history` service of the
local part opens every run in `runs/events/`:

```bash
cd ../local
docker compose up -d spark-history                      # http://localhost:18080
```

The SQL tab shows the plan with the rows that went through each step, and the Stages tab how long
every task took. In the Jobs tab, the work of Sedona appears under the names of its own code,
`aggregate at SpatialRDD.java` to count the points, `collect at SpatialRDD.java` to sample them and
`flatMapToPair at SpatialRDD.java` to send them to their cells.

## The data

| Source | What | Slice used | Format | License |
|---|---|---|---|---|
| [GBIF](https://www.gbif.org) | Occurrences of species, from museums, herbaria and citizen science apps | 3,635,163 records in Castilla-La Mancha | Parquet | CC0, CC BY or CC BY-NC, depending on the dataset |
| [Overture Maps](https://overturemaps.org), divisions | Administrative areas, from countries to neighborhoods | 4,557 areas in the bounding box, 926 municipalities of Castilla-La Mancha | GeoParquet | ODbL |
| [Copernicus DEM](https://registry.opendata.aws/copernicus-dem/) GLO-30 | Elevation of the ground every 30 m | 24 tiles of 1x1 degree | Cloud Optimized GeoTIFF | Free for any use, with attribution |

The cloud part reads the monthly snapshot of GBIF from `s3://gbif-open-data-us-east-1`, which only
holds the records under CC0 and CC BY, and the release of Overture from
`s3://overturemaps-us-west-2`. The versions are set by `gbif_snapshot` and `overture_release` in
the Terraform variables, since Overture only keeps its latest releases.

A snapshot with a date is not necessarily frozen. The one of 1 October 2026 was published on the
5th with 9,926 files, and on the 23rd GBIF added the 522 that were missing, 15 GB or about 5% of
the records. Runs of Spain before and after that day gave 102.8 and 108.9 million observations.
Spark records the number of files it read in the plan of every run, and for results that must be
reproducible it is safer to keep that number, or a copy of the snapshot.

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

The cloud part uses what EMR ships, plus Sedona from Maven Central when the cluster starts.

| Component | Version |
|---|---|
| Amazon EMR | `emr-spark-8.1.0`, with Spark 4.1.1 and Scala 2.13 |
| Apache Sedona | 1.9.1, `sedona-spark-shaded-4.1_2.13` and `geotools-wrapper` |
| Terraform | 1.9 or later, with the AWS provider 6.68 |
| sbt | 1.11.7, in the `sbtscala/scala-sbt` image |

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

**Graviton instances.** On `c7g` every vCPU is a physical core, while on the Intel instances two
vCPU share one, and a spatial join is all CPU. They were also the cheapest on spot when this was
written, between 50% and 70% off, and the ones AWS takes back the least, under 5% a month against
more than 20% for the newer `c8g` and `c7i`.

**Trino 483, not 484.** 484 came out the day Trino was added to the demo, so the previous release
felt like the safer choice.

## Known limitations

These are limits of the versions and the data above, not of the demo setup. I checked each of them
against the code or reproduced it.

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
- **Overture mixes levels under `locality`.** In Spain the subtype holds municipalities, but also
  comarcas that group several of them and some units below the municipality, with nothing in the
  data that tells them apart. 361 of the 8,655 localities contain others, so an observation in
  Figueres counts for Figueres and for Alt Empordà, and the observations of the result add up to
  more than the points that went in. The comarcas are also the first rows when sorting by
  observations, and other countries have similar cases.
- **Sedona's grid cannot split identical points.** Its KDB-tree sorts a full cell again with every
  new point that lands in it when the cell cannot be split, which with the repeated coordinates of
  GBIF kept the driver busy for minutes. Grouping the observations by coordinate first avoids it.
- **`spark.sedona.join.approxcount` empties the join.** In Sedona 1.9.1 it makes the `RangeJoin`
  skip counting the points, but the count it gives is never passed on, and without a count the
  points are not partitioned and the join returns no rows. I found it reading the code, not running
  it.
- **Points on the cuts of a polygon are lost.** `ST_Contains` does not include the boundary, so a
  point exactly on the line between two pieces of `ST_SubDivideExplode` belongs to neither. In
  Spain it happened to 68 observations out of 102.8 million.
- **The `RangeJoin` has no metrics.** Its box in the SQL tab of the Spark UI stays empty, so the
  number of pairs that come out of the join is only visible in the result.
- **Terraform cannot turn on two features of EMR yet**: sending the logs to CloudWatch, which would
  show the driver and the executors while they run
  ([hashicorp/terraform-provider-aws#49317](https://github.com/hashicorp/terraform-provider-aws/issues/49317)),
  and Spark Connect sessions
  ([hashicorp/terraform-provider-aws#50090](https://github.com/hashicorp/terraform-provider-aws/issues/50090)).
- **The list columns of GBIF need an odd type in Glue.** The Parquet files use the old Hive layout
  for lists, which Athena reads as `array<struct<array_element:string>>` and not as the
  `array<string>` the GBIF documentation gives.
- **Trino cannot read `bronze.overture_division_area`.** It is registered as GeoParquet, a format
  only Spark with Sedona understands. The GBIF table, in plain Parquet, it reads fine.

## This is not a production setup

The local part is tuned so that a laptop can run it and a person can follow it.

- **A slice of the data.** One region, so that every job runs in a minute or two.
- **Spark in local mode.** Every job runs in a single JVM on the cores of the laptop.
- **A lookback window of one year.** `bronze_to_silver` only reads the downloads of the last 365
  days. A real pipeline would read only what arrived since the last run.
- **Throwaway state.** The Hive Metastore keeps its catalog in an embedded Derby database inside
  its container. `docker compose down` forgets the tables while their files stay in RustFS, and
  `docker compose down -v` removes the files too. After a `down`, `load-bronze.sh` and the jobs
  register everything again.
- **No security.** There is no TLS and no authentication between the services, and the RustFS
  credentials are the same for everything.

The cloud part is much closer to how it would run for real: the data stays where it is published,
the infrastructure is code, the nodes can only touch what they need, and the cluster only lives
for the run. A few shortcuts are left on purpose. Terraform keeps its state in a local file, the
cluster runs in a public subnet, and `terraform destroy` takes the bucket and the results with it.

## Repository layout

Each folder of `local/` holds everything its containers need. In both parts, `scripts/` holds
what you run from your own machine.

```
.
├── README.md
├── docs/images/              logos used in this README
├── cloud/
│   ├── terraform/            VPC, bucket, Glue, Athena, IAM and the EMR cluster, one file per service
│   ├── jobs/                 sbt project with the Scala job, ObsMunicipality
│   ├── scripts/
│   │   ├── build.sh          builds the jar inside Docker
│   │   ├── submit.sh         uploads the jar and runs one of its classes as an EMR step
│   │   └── save-run.sh       downloads the Spark event logs and the EMR logs of a run
│   └── runs/                 the saved runs, not committed
└── local/
    ├── docker-compose.yml    services, ports, volumes, dependencies
    ├── .env.example          GBIF account and RustFS credentials, copied to .env
    ├── rustfs/               S3 storage settings and the script that creates the bucket
    ├── metastore/            Hive Metastore image with S3A, and its core-site.xml
    ├── spark/                Spark image (Sedona, Iceberg, S3A, R), spark-defaults.conf, logging
    ├── trino/                Trino catalogs for Iceberg and Hive
    ├── spark-history/        settings of the history server that opens cloud/runs
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
| Region of the data | `BBOX` and `GADM_GID` in [`download-data.sh`](local/scripts/download-data.sh), and the region code in the gold jobs | Castilla-La Mancha |
| Lookback window of silver | `LOOKBACK_DAYS` in `bronze_to_silver` | 365 days |
| Size of the raster pieces | `RS_TileExplode` in `silver_gold_terrain` | 512 x 512 pixels |
| Spark memory | `spark.driver.memory` in [`spark-defaults.conf`](local/spark/spark-defaults.conf), and `connect()` in [`jobs/r/lakehouse.R`](local/jobs/r/lakehouse.R) | 6 GB |
| Size of the EMR cluster | `primary_instance_type`, `worker_instance_type`, `core_count` and `task_count` in [`variables.tf`](cloud/terraform/variables.tf) | `m7g.2xlarge`, and 1 + 5 `c7g.8xlarge` |
| Spot or on demand | `task_on_spot` in `variables.tf` | spot |
| Size of the polygon pieces | `ST_SubDivideExplode` in `ObsMunicipality.scala` | 256 vertices |
| Rows per cell of the spatial join | `PointsPerCell` in `ObsMunicipality.scala` | 20,000 |

## Troubleshooting

**A port is already in use.** The demo needs 4040, 8080, 9001 and 18080 on the host.

**A job dies with `OutOfMemoryError`.** Give Docker more memory, or stop Trino while the job runs
with `docker compose stop trino`. From R, keep the driver memory in `connect()`, because in local
mode sparklyr passes its own `--driver-memory 2g`, which wins over `spark-defaults.conf`.

**The GBIF download is not in your GBIF account yet.** The script downloads Overture and the DEM
first, and only then asks GBIF for the occurrences.

**Spark fails to infer the partitions of bronze.** This happens if the bucket holds folders with
two different partition schemes, for example after changing them. Start from scratch with
`docker compose down -v` and run `load-bronze.sh` again.

**The EMR cluster stops with `VALIDATION_ERROR: Service role ... has insufficient EC2 permissions`.**
The managed policy of the EMR service only lets it create instances tagged with
`for-use-with-amazon-emr-managed-policies = true`, and EMR copies that tag from the cluster, so
check that it is still in the tags of the cluster, the subnet and the security groups. CloudTrail
shows the exact call that was denied. Right after the IAM roles are created the same error can also
come from IAM taking a few seconds to spread, and running `apply` again solves it.

**The task nodes never start.** There may be no spot capacity of that type at the time. Set
`task_on_spot` to `false` and run `terraform apply` again.

**The driver is stuck after `collect at SpatialRDD`, with the executors idle.** That is Sedona
drawing its grid alone on the driver. If it takes more than a minute, the side that draws the grid
probably has many identical points, and grouping them before the join solves it.

**The Spark History Server of the EMR console fails while a job runs.** It reads the event log
while the driver is still writing it. The log of the step in S3, updated every few minutes, and the
history server once the job is done are more reliable.

**The metastore is slow to start on a Mac.** It runs under amd64 emulation. Make sure Rosetta is
enabled in Docker Desktop (Settings → General → "Use Rosetta for x86_64/amd64 emulation").

## Stopping

```bash
docker compose stop       # pause, keeping the state (resume with: docker compose start)
docker compose down -v    # remove everything, including the lakehouse in RustFS
```

In the cloud part, `terraform destroy` from `cloud/terraform` removes the cluster, the bucket and
everything else it created.

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
the Trino Software Foundation, and RustFS and its logo to their owners. Amazon EMR, AWS Glue, Amazon
Athena, Amazon S3 and their icons, taken from the
[AWS Architecture Icons](https://aws.amazon.com/architecture/icons/), are trademarks of
Amazon.com, Inc. They are used here only to identify each project or service.</sub>
