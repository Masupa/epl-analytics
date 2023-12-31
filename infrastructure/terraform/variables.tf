variable "project" {
  description = "Your GCP Project ID"
}

locals {
  data_lake_bucket = "epl_data_lake"
  player_stats_schema = file("./resources/schema/player_stats_data.json")
  player_demo_schema = file("./resources/schema/player_data_football_critic.json")
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

variable "storage_class" {
  default = "STANDARD"
  description = "Storage class type for the bucket"
}

