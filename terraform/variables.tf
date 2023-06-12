variable "project" {}

locals {
  data_lake_bucket = "epl_data_lake"
}

variable "credentials_file" {}

variable "region" {
  default     = "us-central1"
  type        = string
  description = "Cloud location for the project's resources"
}

variable "zone" {
  default     = "us-central1-c"
  type        = string
  description = "Cloud zone for the project's resources"
}

variable "big_query_dataset" {
  default     = "epl_analytics_dwh"
  type        = string
  description = "BigQuery dataset that raw data from GCS will be written to"
}

