output "s3_bucket_data" {
  description = "S3 bucket for data storage"
  value       = aws_s3_bucket.data_parquet.bucket
}

output "s3_bucket_scripts" {
  description = "S3 bucket for scripts"
  value       = aws_s3_bucket.scripts_gluejob.bucket
}

output "glue_job_name" {
  description = "AWS Glue job name"
  value       = aws_glue_job.python_job.name
}

output "glue_job_arn" {
  description = "AWS Glue job ARN"
  value       = aws_glue_job.python_job.arn
}

output "snowflake_database" {
  description = "Snowflake database name"
  value       = snowflake_database.vector_db.name
}

output "snowflake_schema" {
  description = "Snowflake schema name"
  value       = snowflake_schema.embeddings_schema.name
}

output "snowflake_table" {
  description = "Snowflake table name"
  value       = snowflake_table.document_chunks.name
}

output "snowflake_warehouse" {
  description = "Snowflake warehouse name"
  value       = snowflake_warehouse.etl_wh.name
}
