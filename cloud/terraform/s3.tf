resource "aws_s3_bucket" "lakehouse" {
  bucket        = "${data.aws_caller_identity.current.account_id}-spatial-lakehouse"
  force_destroy = true

  # Do not use force_destroy in production, 
  # as it will delete all the data in the bucket when destroying the stack

  tags = {
    Module = var.storage_tag
  }
}

locals {
  data_uri = "s3://${aws_s3_bucket.lakehouse.id}/data"
  code_uri = "s3://${aws_s3_bucket.lakehouse.id}/code"
  temp_uri = "s3://${aws_s3_bucket.lakehouse.id}/temp"

  temp_expiration_days = {
    athena       = 15
    logs         = 30
    spark-events = 30
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  dynamic "rule" {
    for_each = local.temp_expiration_days

    content {
      id     = "expire-temp-${rule.key}"
      status = "Enabled"

      filter {
        prefix = "temp/${rule.key}/"
      }

      expiration {
        days = rule.value
      }
    }
  }
}
