output "bucket_name" {
  description = "S3 bucket name"
  value       = aws_s3_bucket.lab.id
}

output "glue_database" {
  description = "Base Glue"
  value       = aws_glue_catalog_database.lab.name
}

output "athena_workgroup" {
  description = "Workgroup Athena"
  value       = aws_athena_workgroup.lab.name
}