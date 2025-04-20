terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "us-east-1"
}

# Tags for all resources
locals {
  common_tags = {
    Project     = "BigDataInfrastructure"
    Environment = "Production"
    ManagedBy   = "Terraform"
  }
} 