output "bucket" {
  value = aws_s3_bucket.lakehouse.id
}

output "cluster_id" {
  value = aws_emr_cluster.sedona.id
}

output "code_uri" {
  value = local.code_uri
}

output "region" {
  value = var.region
}

output "temp_uri" {
  value = local.temp_uri
}
