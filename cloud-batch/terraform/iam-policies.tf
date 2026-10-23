resource "aws_iam_policy" "emr_service_pass_role" {
  name        = "${var.project}-emr-pass-instance-role"
  description = "Lets the EMR service hand the instance role to the nodes it creates"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "iam:PassRole"
      Resource  = aws_iam_role.emr_instance.arn
      Condition = { StringEquals = { "iam:PassedToService" = "ec2.amazonaws.com" } }
    }]
  })

  tags = {
    Module = var.compute_tag
  }
}

resource "aws_iam_policy" "emr_instance_s3" {
  name        = "${var.project}-emr-instance-s3"
  description = "Read and write the lakehouse bucket, and read GBIF, Overture and the EMR tools"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "LakehouseBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
        ]
        Resource = [aws_s3_bucket.lakehouse.arn, "${aws_s3_bucket.lakehouse.arn}/*"]
      },
      {
        Sid    = "PublicData"
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetBucketLocation", "s3:GetObject"]
        Resource = [
          "arn:aws:s3:::gbif-open-data-us-east-1",
          "arn:aws:s3:::gbif-open-data-us-east-1/*",
          "arn:aws:s3:::overturemaps-us-west-2",
          "arn:aws:s3:::overturemaps-us-west-2/*",
        ]
      },
      {
        Sid      = "EmrBuckets"
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = ["arn:aws:s3:::*.elasticmapreduce/*", "arn:aws:s3:::elasticmapreduce/*"]
      },
    ]
  })

  tags = {
    Module = var.compute_tag
  }
}

resource "aws_iam_policy" "emr_instance_glue" {
  name        = "${var.project}-emr-instance-glue"
  description = "Read the tables of the Glue catalog and write the gold ones"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:BatchGetPartition",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetUserDefinedFunctions",
        "glue:CreateDatabase",
      ]
      Resource = [
        "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
        "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/*",
        "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/*",
      ]
    }]
  })

  tags = {
    Module = var.compute_tag
  }
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2"
}

resource "aws_iam_role_policy_attachment" "emr_service_pass_role" {
  role       = aws_iam_role.emr_service.name
  policy_arn = aws_iam_policy.emr_service_pass_role.arn
}

resource "aws_iam_role_policy_attachment" "emr_instance_s3" {
  role       = aws_iam_role.emr_instance.name
  policy_arn = aws_iam_policy.emr_instance_s3.arn
}

resource "aws_iam_role_policy_attachment" "emr_instance_glue" {
  role       = aws_iam_role.emr_instance.name
  policy_arn = aws_iam_policy.emr_instance_glue.arn
}

resource "aws_iam_role_policy_attachment" "emr_instance_ssm" {
  role       = aws_iam_role.emr_instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}
