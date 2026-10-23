resource "aws_glue_catalog_database" "layer" {
  for_each = toset(["bronze", "silver", "gold"])

  name         = each.key
  description  = "${title(each.key)} layer of the geospatial lakehouse"
  catalog_id   = data.aws_caller_identity.current.account_id
  location_uri = "${local.data_uri}/${each.key}/"

  tags = {
    Module = var.catalog_tag
  }
}

locals {
  bronze_tables = {
    gbif_occurrence = {
      location = "s3://gbif-open-data-us-east-1/occurrence/${var.gbif_snapshot}/occurrence.parquet/"
      columns = [
        { name = "gbifid", type = "string" },
        { name = "datasetkey", type = "string" },
        { name = "occurrenceid", type = "string" },
        { name = "kingdom", type = "string" },
        { name = "phylum", type = "string" },
        { name = "class", type = "string" },
        { name = "order", type = "string" },
        { name = "family", type = "string" },
        { name = "genus", type = "string" },
        { name = "species", type = "string" },
        { name = "infraspecificepithet", type = "string" },
        { name = "taxonrank", type = "string" },
        { name = "scientificname", type = "string" },
        { name = "verbatimscientificname", type = "string" },
        { name = "verbatimscientificnameauthorship", type = "string" },
        { name = "countrycode", type = "string" },
        { name = "locality", type = "string" },
        { name = "stateprovince", type = "string" },
        { name = "occurrencestatus", type = "string" },
        { name = "individualcount", type = "int" },
        { name = "publishingorgkey", type = "string" },
        { name = "decimallatitude", type = "double" },
        { name = "decimallongitude", type = "double" },
        { name = "coordinateuncertaintyinmeters", type = "double" },
        { name = "coordinateprecision", type = "double" },
        { name = "elevation", type = "double" },
        { name = "elevationaccuracy", type = "double" },
        { name = "depth", type = "double" },
        { name = "depthaccuracy", type = "double" },
        { name = "eventdate", type = "timestamp" },
        { name = "day", type = "int" },
        { name = "month", type = "int" },
        { name = "year", type = "int" },
        { name = "taxonkey", type = "string" },
        { name = "specieskey", type = "string" },
        { name = "basisofrecord", type = "string" },
        { name = "institutioncode", type = "string" },
        { name = "collectioncode", type = "string" },
        { name = "catalognumber", type = "string" },
        { name = "recordnumber", type = "string" },
        { name = "identifiedby", type = "array<struct<array_element:string>>" },
        { name = "dateidentified", type = "timestamp" },
        { name = "license", type = "string" },
        { name = "rightsholder", type = "string" },
        { name = "recordedby", type = "array<struct<array_element:string>>" },
        { name = "typestatus", type = "array<struct<array_element:string>>" },
        { name = "establishmentmeans", type = "string" },
        { name = "lastinterpreted", type = "timestamp" },
        { name = "mediatype", type = "array<struct<array_element:string>>" },
        { name = "issue", type = "array<struct<array_element:string>>" },
      ]
    }

    overture_division_area = {
      location = "s3://overturemaps-us-west-2/release/${var.overture_release}/theme=divisions/type=division_area/"
      columns = [
        { name = "id", type = "string" },
        { name = "geometry", type = "binary" },
        { name = "country", type = "string" },
        { name = "sources", type = "array<struct<property:string,dataset:string,license:string,record_id:string,update_time:string,confidence:double,between:array<double>,provider:string,resource:string,version:string>>" },
        { name = "subtype", type = "string" },
        { name = "admin_level", type = "int" },
        { name = "class", type = "string" },
        { name = "names", type = "struct<primary:string,common:map<string,string>,rules:array<struct<variant:string,language:string,perspectives:struct<mode:string,countries:array<string>>,value:string,between:array<double>,side:string>>>" },
        { name = "is_land", type = "boolean" },
        { name = "is_territorial", type = "boolean" },
        { name = "region", type = "string" },
        { name = "division_id", type = "string" },
        { name = "version", type = "int" },
        { name = "bbox", type = "struct<xmin:double,xmax:double,ymin:double,ymax:double>" },
      ]
    }
  }
}

resource "aws_glue_catalog_table" "bronze" {
  for_each = local.bronze_tables

  name          = each.key
  database_name = aws_glue_catalog_database.layer["bronze"].name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = each.value.location
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = each.value.columns

      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

locals {
  gold_tables = {
    obs_municipality = {
      columns = [
        { name = "scope", type = "string" },
        { name = "municipality_id", type = "string" },
        { name = "name", type = "string" },
        { name = "country", type = "string" },
        { name = "region", type = "string" },
        { name = "observations", type = "bigint" },
        { name = "species", type = "bigint" },
        { name = "geometry", type = "binary" },
      ]
    }
  }
}

resource "aws_glue_catalog_table" "gold" {
  for_each = local.gold_tables

  name          = each.key
  database_name = aws_glue_catalog_database.layer["gold"].name
  table_type    = "EXTERNAL_TABLE"

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = 2
    }
  }

  storage_descriptor {
    location = "${local.data_uri}/gold/${each.key}"

    dynamic "columns" {
      for_each = each.value.columns

      content {
        name = columns.value.name
        type = columns.value.type
        parameters = {
          "iceberg.field.current"  = "true"
          "iceberg.field.id"       = tostring(columns.key + 1)
          "iceberg.field.optional" = "true"
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [parameters]
  }
}
