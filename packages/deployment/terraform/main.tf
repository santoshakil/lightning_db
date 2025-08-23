terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
  }
  
  backend "s3" {
    # Configure remote state backend
    bucket         = "lightning-db-terraform-state"
    key            = "production/terraform.tfstate"
    region         = "us-west-2"
    encrypt        = true
    dynamodb_table = "terraform-state-lock"
  }
}

# Variables
variable "environment" {
  description = "Environment name"
  type        = string
  default     = "production"
}

variable "cloud_provider" {
  description = "Cloud provider (aws, gcp, azure)"
  type        = string
  default     = "aws"
}

variable "region" {
  description = "Cloud region"
  type        = string
  default     = "us-west-2"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "lightning-db"
}

variable "instance_count" {
  description = "Number of Lightning DB instances"
  type        = number
  default     = 3
}

variable "enable_monitoring" {
  description = "Enable monitoring stack"
  type        = bool
  default     = true
}

variable "enable_backup" {
  description = "Enable automated backups"
  type        = bool
  default     = true
}

# Local values
locals {
  common_tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "terraform"
  }
}

# Module calls based on cloud provider
module "aws_infrastructure" {
  count  = var.cloud_provider == "aws" ? 1 : 0
  source = "./aws"
  
  environment      = var.environment
  project_name     = var.project_name
  region          = var.region
  instance_count  = var.instance_count
  enable_monitoring = var.enable_monitoring
  enable_backup   = var.enable_backup
  tags            = local.common_tags
}

module "gcp_infrastructure" {
  count  = var.cloud_provider == "gcp" ? 1 : 0
  source = "./gcp"
  
  environment      = var.environment
  project_name     = var.project_name
  region          = var.region
  instance_count  = var.instance_count
  enable_monitoring = var.enable_monitoring
  enable_backup   = var.enable_backup
}

module "azure_infrastructure" {
  count  = var.cloud_provider == "azure" ? 1 : 0
  source = "./azure"
  
  environment      = var.environment
  project_name     = var.project_name
  region          = var.region
  instance_count  = var.instance_count
  enable_monitoring = var.enable_monitoring
  enable_backup   = var.enable_backup
  tags            = local.common_tags
}

# Outputs
output "cluster_endpoint" {
  description = "Kubernetes cluster endpoint"
  value = var.cloud_provider == "aws" ? (
    length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].cluster_endpoint : null
  ) : var.cloud_provider == "gcp" ? (
    length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].cluster_endpoint : null
  ) : var.cloud_provider == "azure" ? (
    length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].cluster_endpoint : null
  ) : null
}

output "database_endpoint" {
  description = "Lightning DB service endpoint"
  value = var.cloud_provider == "aws" ? (
    length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].database_endpoint : null
  ) : var.cloud_provider == "gcp" ? (
    length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].database_endpoint : null
  ) : var.cloud_provider == "azure" ? (
    length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].database_endpoint : null
  ) : null
}

output "monitoring_endpoint" {
  description = "Monitoring dashboard endpoint"
  value = var.enable_monitoring ? (
    var.cloud_provider == "aws" ? (
      length(module.aws_infrastructure) > 0 ? module.aws_infrastructure[0].monitoring_endpoint : null
    ) : var.cloud_provider == "gcp" ? (
      length(module.gcp_infrastructure) > 0 ? module.gcp_infrastructure[0].monitoring_endpoint : null
    ) : var.cloud_provider == "azure" ? (
      length(module.azure_infrastructure) > 0 ? module.azure_infrastructure[0].monitoring_endpoint : null
    ) : null
  ) : null
}