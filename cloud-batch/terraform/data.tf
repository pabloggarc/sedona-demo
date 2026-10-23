data "aws_caller_identity" "current" {}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_pricing_product" "worker" {
  service_code = "AmazonEC2"

  filters {
    field = "instanceType"
    value = var.worker_instance_type
  }

  filters {
    field = "regionCode"
    value = var.region
  }

  filters {
    field = "operatingSystem"
    value = "Linux"
  }

  filters {
    field = "tenancy"
    value = "Shared"
  }

  filters {
    field = "preInstalledSw"
    value = "NA"
  }

  filters {
    field = "capacitystatus"
    value = "Used"
  }
}
