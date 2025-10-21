resource "aws_s3_bucket" "data_parquet" {
  bucket = var.aws_bucket_name
  tags = {
    name = "data_parquet"
    Environment= "dev"
  }
}

resource "aws_s3_bucket" "scripts_gluejob" {
  bucket = var.aws_bucket_scripts
  tags = {
    Name        = "data_scripts"
    Environment = "dev"
  }
}

resource "aws_s3_object" "python_job" {
  bucket        = aws_s3_bucket.scripts_gluejob.id  # o .bucket
  key           = "scripts/python_job.py"
  source        = "${path.module}/../scripts/python_job.py"
  content_type  = "text/x-python"
}


# ---------------------------
# IAM Role para Glue
# ---------------------------
resource "aws_iam_role" "glue_role" {
  name = "facu-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Service = "glue.amazonaws.com"
      },
      Action = "sts:AssumeRole"
    }]
  })
}

# Permisos para Glue: acceso a S3 + CloudWatch logs
resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_logs_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

#glue/python

resource "aws_glue_job" "python_job" {
  name     = var.aws_gluejob_name
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "pythonshell"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.scripts_gluejob.bucket}/scripts/python_job.py"
  }

  default_arguments = {
    "--additional-python-modules" = "snowflake-connector-python,pandas,pyarrow,boto3"
    "--job-language"              = "python"
    "--enable-metrics"             = "true"
  }

  max_retries       = 2
  timeout           = 60  # Aumentar timeout para procesamiento de embeddings
  glue_version      = "4.0"  # Usar versión más reciente
  number_of_workers = 2
  worker_type       = "Standard"

  tags = {
    Environment = "dev"
    Name        = "python_glue_job"
    Project     = "macro-etl"
  }
}

#snowflake 


#creation data base
resource "snowflake_database" "vector_db" {
  name = var.snowflake_database
  comment = "data base for embeddings"
}

#creation schema
resource "snowflake_schema" "embeddings_schema" {
  database = snowflake_database.vector_db.name
  name = var.snowflake_schema
}

#create warehouse
resource "snowflake_warehouse" "etl_wh" {
  name = var.snowflake_warehouse
  warehouse_size = "XSMALL"
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true
}
#create vectorial daba base

resource "snowflake_table" "document_chunks" {
  database = snowflake_database.vector_db.name
  schema = snowflake_schema.embeddings_schema.name
  name = "DOCUMENT_CHUNKS"

  column {
    name = "CHUNK_ID"
    type = "VARCHAR"
  }
  column {
    name = "CHUNK_NAME"
    type = "VARCHAR"
  }
  # Usar VARIANT para embeddings si VECTOR no está disponible en tu región
  column {
    name = var.embed_temp
    type = "VARIANT"
  }
  # Comentario: Si VECTOR está disponible en tu región, descomenta la siguiente línea
  # column {
  #   name = "EMBEDDING"
  #   type = "VECTOR(FLOAT, 768)"
  # }
}