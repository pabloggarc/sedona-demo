resource "aws_security_group" "emr_primary" {
  name                   = "${var.project}-emr-primary"
  description            = "Primary node of the EMR cluster, managed by EMR"
  vpc_id                 = aws_vpc.main.id
  revoke_rules_on_delete = true

  tags = {
    Module = var.network_tag

    "for-use-with-amazon-emr-managed-policies" = "true"
  }

  lifecycle {
    ignore_changes = [ingress]
  }
}

resource "aws_security_group" "emr_workers" {
  name                   = "${var.project}-emr-workers"
  description            = "Core and task nodes of the EMR cluster, managed by EMR"
  vpc_id                 = aws_vpc.main.id
  revoke_rules_on_delete = true

  tags = {
    Module = var.network_tag

    "for-use-with-amazon-emr-managed-policies" = "true"
  }

  lifecycle {
    ignore_changes = [ingress]
  }
}

resource "aws_vpc_security_group_egress_rule" "emr_primary_all" {
  security_group_id = aws_security_group.emr_primary.id
  description       = "All outbound traffic, to S3, Glue, Maven Central and the EMR service"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"

  tags = {
    Module = var.network_tag
  }
}

resource "aws_vpc_security_group_egress_rule" "emr_workers_all" {
  security_group_id = aws_security_group.emr_workers.id
  description       = "All outbound traffic, to S3, Glue, Maven Central and the EMR service"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"

  tags = {
    Module = var.network_tag
  }
}
