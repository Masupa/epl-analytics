# Terraform Block
terraform {
  required_version = ">=1.0"
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  credentials = var.credentials_file

  project = var.project
  region  = var.region
  zone    = var.zone
}

# Data Lake
resource "google_storage_bucket" "data_lake_bucket" {
  name     = "${local.data_lake_bucket}_${var.project}"
  location = var.region

  storage_class = var.storage_class
  uniform_bucket_level_access = true
}

# DWH
resource "google_bigquery_dataset" "epl_dataset" {
  dataset_id = var.big_query_dataset
  project    = var.project
  location   = var.region
}

# Player Stats Table
resource "google_bigquery_table" "player_stats_data" {
  project = var.project
  dataset_id = var.big_query_dataset
  table_id = "player_stats_data"

  schema = local.player_stats_schema

  # Define clustering configuration
  clustering = ["Season"]

  }

# Player Demographics Table
resource "google_bigquery_table" "player_demo_data" {
  project = var.project
  dataset_id = var.big_query_dataset
  table_id = "player_demo_data"

  schema = local.player_demo_schema

  deletion_protection = false

  # Define clustering configuration
  clustering = ["Season", "Club", "Position", "Nationality"]

  }
