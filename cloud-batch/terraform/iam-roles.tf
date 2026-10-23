resource "aws_iam_role" "emr_service" {
  name = "${var.project}-emr-service"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = {
    Module = var.compute_tag
  }
}

resource "aws_iam_role" "emr_instance" {
  name = "${var.project}-emr-instance"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = {
    Module = var.compute_tag
  }
}

resource "aws_iam_instance_profile" "emr_instance" {
  name = "${var.project}-emr-instance"
  role = aws_iam_role.emr_instance.name

  tags = {
    Module = var.compute_tag
  }
}
