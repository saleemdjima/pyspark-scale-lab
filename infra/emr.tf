# EMR Serverless application and execution role.

variable "emr_release" {
  description = "EMR Serverless release label used by the Spark application"
  type        = string
  default     = "emr-7.5.0"
}

# Allow the EMR Serverless service to assume the Spark execution role.
data "aws_iam_policy_document" "emr_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_exec" {
  name               = "${var.project}-emr-exec"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}

# Grant access to the lab bucket, CloudWatch Logs, and the optional Glue catalog.
data "aws_iam_policy_document" "emr_exec_perms" {
  statement {
    sid    = "S3ReadWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.lab.arn,
      "${aws_s3_bucket.lab.arn}/*",
    ]
  }
  statement {
    sid    = "Logs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
      "logs:DescribeLogStreams", "logs:DescribeLogGroups",
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }
  statement {
    sid       = "GlueReadOptional"
    effect    = "Allow"
    actions   = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
    resources = ["*"]
  }
}

# Attach the execution permissions to the EMR Serverless role.
resource "aws_iam_role_policy" "emr_exec" {
  name   = "${var.project}-emr-exec-policy"
  role   = aws_iam_role.emr_exec.id
  policy = data.aws_iam_policy_document.emr_exec_perms.json
}

# Provision a Spark application that stops automatically after inactivity.
resource "aws_emrserverless_application" "spark" {
  name          = "${var.project}-spark"
  type          = "spark"
  release_label = var.emr_release

  # Avoid idle compute charges by stopping the application after 15 minutes.
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }
}

# Application and role identifiers for job submission scripts.
output "emr_application_id" {
  description = "EMR Serverless Spark application ID"
  value       = aws_emrserverless_application.spark.id
}

output "emr_execution_role_arn" {
  description = "ARN of the IAM role used to execute Spark jobs"
  value       = aws_iam_role.emr_exec.arn
}