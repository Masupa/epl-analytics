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
}

# DWH
resource "google_bigquery_dataset" "epl_dataset" {
  dataset_id = var.big_query_dataset
  project    = var.project
  location   = var.region
}
