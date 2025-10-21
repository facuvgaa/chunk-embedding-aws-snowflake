provider "aws" {
  region = var.aws_region
}

terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = ">= 0.104.0"  // Usa la versión más reciente que soporte preview_features_enabled
    }
  }
}
provider "snowflake" {
  account_name         = var.snowflake_account
  organization_name    = var.snowflake_organization
  user                 = var.snowflake_user
  password             = var.snowflake_password
  role                 = var.snowflake_role
  authenticator        = "Snowflake" # Standard Snowflake authentication
  warehouse            = var.snowflake_warehouse
  preview_features_enabled = ["snowflake_table_resource"] # Enable this if the table resource is still in preview
}
