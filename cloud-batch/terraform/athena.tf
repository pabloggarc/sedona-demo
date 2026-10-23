resource "aws_athena_workgroup" "lakehouse" {
  name          = "geospatial-lakehouse"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = 10 * 1024 * 1024 * 1024

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "${local.temp_uri}/athena/"
    }
  }

  tags = {
    Module = var.query_tag
  }
}
