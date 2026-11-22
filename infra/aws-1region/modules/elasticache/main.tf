# Valkey, cluster mode disabled: the Lua script touches two keys (entry + table
# watermark), which needs a single shard. TLS + AUTH + CMK.
terraform {
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.73"
    }
  }
}

variable "name" { type = string }
variable "subnet_ids" { type = list(string) }
variable "security_group_id" { type = string }
variable "kms_key_arn" { type = string }
variable "auth_secret_arn" { type = string }
variable "node_count" { type = number }
variable "node_type" { type = string }
variable "engine_version" { type = string }

resource "aws_elasticache_subnet_group" "this" {
  name       = var.name
  subnet_ids = var.subnet_ids
}

# volatile-lru: only entries with a TTL are evictable, never cdc:epoch or the
# truncate watermark.
resource "aws_elasticache_parameter_group" "this" {
  name   = var.name
  family = "valkey${split(".", var.engine_version)[0]}"

  parameter {
    name  = "maxmemory-policy"
    value = "volatile-lru"
  }
}

ephemeral "aws_secretsmanager_secret_version" "auth" {
  secret_id = var.auth_secret_arn
}

resource "aws_elasticache_replication_group" "this" {
  #checkov:skip=CKV2_AWS_50:side B is a single node by design (plan cost table); side A is multi-AZ
  #checkov:skip=CKV_AWS_134:cache content is rebuilt from CDC; backups add nothing
  #checkov:skip=CKV_AWS_31:AUTH is set through auth_token_wo, which checkov does not evaluate
  replication_group_id       = var.name
  description                = "${var.name} CDC cache"
  engine                     = "valkey"
  engine_version             = var.engine_version
  node_type                  = var.node_type
  num_cache_clusters         = var.node_count
  automatic_failover_enabled = var.node_count > 1
  multi_az_enabled           = var.node_count > 1
  parameter_group_name       = aws_elasticache_parameter_group.this.name
  subnet_group_name          = aws_elasticache_subnet_group.this.name
  security_group_ids         = [var.security_group_id]
  at_rest_encryption_enabled = true
  kms_key_id                 = var.kms_key_arn
  transit_encryption_enabled = true
  transit_encryption_mode    = "required"
  auth_token_wo              = ephemeral.aws_secretsmanager_secret_version.auth.secret_string
  auth_token_wo_version      = 1
  snapshot_retention_limit   = 0
  apply_immediately          = true
}

output "endpoint" { value = aws_elasticache_replication_group.this.primary_endpoint_address }
