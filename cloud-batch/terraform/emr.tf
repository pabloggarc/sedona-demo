resource "aws_emr_cluster" "sedona" {
  name          = "sedona-demo"
  release_label = var.release_label
  applications  = ["Spark"]
  service_role  = aws_iam_role.emr_service.arn
  log_uri       = "${local.temp_uri}/logs/"

  ec2_attributes {
    subnet_id                         = aws_subnet.public.id
    emr_managed_master_security_group = aws_security_group.emr_primary.id
    emr_managed_slave_security_group  = aws_security_group.emr_workers.id
    instance_profile                  = aws_iam_instance_profile.emr_instance.arn
  }

  master_instance_group {
    name          = "primary"
    instance_type = var.primary_instance_type
  }

  core_instance_group {
    name           = "core"
    instance_type  = var.worker_instance_type
    instance_count = var.core_count
  }

  auto_termination_policy {
    idle_timeout = var.idle_timeout_seconds
  }

  configurations_json = jsonencode([
    {
      Classification = "iceberg-defaults"
      Properties = {
        "iceberg.enabled" = "true"
      }
    },
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.jars.packages"                          = "org.apache.sedona:sedona-spark-shaded-4.1_2.13:1.9.1,org.datasyslab:geotools-wrapper:1.9.1-33.5"
        "spark.serializer"                             = "org.apache.spark.serializer.KryoSerializer"
        "spark.kryo.registrator"                       = "org.apache.sedona.core.serde.SedonaKryoRegistrator"
        "spark.sql.extensions"                         = "org.apache.sedona.sql.SedonaSqlExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        "spark.sql.catalog.spark_catalog"              = "org.apache.iceberg.spark.SparkSessionCatalog"
        "spark.sql.catalog.spark_catalog.catalog-impl" = "org.apache.iceberg.aws.glue.GlueCatalog"
        "spark.sql.catalog.spark_catalog.io-impl"      = "org.apache.iceberg.aws.s3.S3FileIO"
        "spark.sql.catalog.spark_catalog.warehouse"    = "${local.data_uri}/"
        "spark.driver.memory"                          = var.driver_memory
      }
    },
    {
      Classification = "spark-hive-site"
      Properties = {
        "hive.metastore.client.factory.class" = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    },
  ])

  tags = {
    Module = var.compute_tag

    "for-use-with-amazon-emr-managed-policies" = "true"
  }

  depends_on = [
    aws_route_table_association.public,
    aws_vpc_security_group_egress_rule.emr_primary_all,
    aws_vpc_security_group_egress_rule.emr_workers_all,
    aws_iam_role_policy_attachment.emr_service,
    aws_iam_role_policy_attachment.emr_service_pass_role,
    aws_iam_role_policy_attachment.emr_instance_s3,
    aws_iam_role_policy_attachment.emr_instance_glue,
  ]

  lifecycle {
    ignore_changes = [os_release_label]
  }
}

locals {
  worker_pricing         = jsondecode(data.aws_pricing_product.worker.result)
  worker_on_demand_term  = one(values(local.worker_pricing.terms.OnDemand))
  worker_price_dimension = one(values(local.worker_on_demand_term.priceDimensions))
  worker_on_demand_price = tostring(tonumber(local.worker_price_dimension.pricePerUnit.USD))
}

resource "aws_emr_instance_group" "task" {
  cluster_id     = aws_emr_cluster.sedona.id
  name           = var.task_on_spot ? "task-spot" : "task-on-demand"
  instance_type  = var.worker_instance_type
  instance_count = var.task_count
  bid_price      = var.task_on_spot ? local.worker_on_demand_price : null
}
