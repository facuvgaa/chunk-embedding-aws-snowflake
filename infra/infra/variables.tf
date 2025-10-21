variable "aws_region" {
    description = "aws region value"
    type = string
    default = "us-east-1"
}

variable "aws_bucket_name" {
    description = "bucket name s3 chunk data"
    type = string
    default = "chunks_data"
}

variable "aws_gluejob_name" {
  description = "gluejob name"
  type = string
  default = "glue_job"
}

variable "aws_bucket_scripts" {
    description = "s3 scripts"
    type = string
    default = "s3_scripts"
}

variable "snowflake_token" {
    description = "token snowflake"
    type = string
}

variable "snowflake_user" {
    description = "user snowflake"
    type = string
}

variable "snowflake_organization" {
  description = "organization name"
  type = string
}

variable "snowflake_password" {
  description = "password snowflake"
  type = string
}

variable "snowflake_account" {
    description = "snowflake account"
    type = string
}

variable "snowflake_database" {
    description = "data base snowflake"
    type = string
}

variable "snowflake_schema" {
    description = "schema snowflake"
    type = string
}

variable "snowflake_warehouse" {
    description = "warehouse snowflake"
    type = string
}

variable "embed_temp" {
    description = "embedding temporal"
    type = string
}

variable "snowflake_role" {
  description = "role snowflake"
  type = string
}
