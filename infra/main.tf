resource "random_id" "suffix" {
  byte_length = 4
}

# Private bucket: S3
resource "aws_s3_bucket" "lab" {
  bucket        = "${var.project}-${random_id.suffix.hex}"
  force_destroy = true
  tags          = { project = var.project }
}

resource "aws_s3_bucket_public_access_block" "lab" {
  bucket                  = aws_s3_bucket.lab.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lab" {
  bucket = aws_s3_bucket.lab.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Glue configuration 
resource "aws_glue_catalog_database" "lab" {
  name = "market_lab"
}

# Athena workgroup SQL configuration 
resource "aws_athena_workgroup" "lab" {
  name          = "${var.project}-wg"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 1073741824

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lab.id}/athena-result/"
    }
  }
}