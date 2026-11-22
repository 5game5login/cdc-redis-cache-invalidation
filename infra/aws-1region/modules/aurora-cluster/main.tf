# Aurora PostgreSQL 16 Serverless v2: one writer + one reader in two AZs.
# With is_secondary=true it becomes the (read-only) Global Database secondary.
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
variable "engine_version" { type = string }
variable "database_name" { type = string }
variable "acu_min" { type = number }
variable "acu_max" { type = number }
variable "log_retention_days" { type = number }

variable "instance_count" {
  type    = number
  default = 2
}

variable "admin_secret_arn" {
  type        = string
  default     = null
  description = "Required unless is_secondary"
}

variable "global_cluster_identifier" {
  type    = string
  default = null
}

variable "is_secondary" {
  type    = bool
  default = false
}

resource "aws_db_subnet_group" "this" {
  name       = var.name
  subnet_ids = var.subnet_ids
}

resource "aws_rds_cluster_parameter_group" "this" {
  name   = var.name
  family = "aurora-postgresql16"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }
  parameter {
    name  = "rds.force_ssl"
    value = "1"
  }
}

# In Aurora, max_slot_wal_keep_size exists only at instance level (verified
# 2026-11-21), so it cannot live in the cluster parameter group. Both instances
# use this group, so the guard survives a failover.
resource "aws_db_parameter_group" "this" {
  name   = var.name
  family = "aurora-postgresql16"

  parameter {
    name  = "max_slot_wal_keep_size"
    value = "2048" # MB
  }
}

# Pre-created so retention and encryption apply from the first line.
resource "aws_cloudwatch_log_group" "postgresql" {
  #checkov:skip=CKV_AWS_338:7-day retention is the plan's choice for a one-time PoC
  name              = "/aws/rds/cluster/${var.name}/postgresql"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn
}

# Read the password back from Secrets Manager rather than passing the generated
# value along: a re-run after a partial apply still uses the stored one.
ephemeral "aws_secretsmanager_secret_version" "admin" {
  count     = var.is_secondary ? 0 : 1
  secret_id = var.admin_secret_arn
}

resource "aws_rds_cluster" "this" {
  #checkov:skip=CKV_AWS_139:PoC must tear down cleanly with terraform destroy
  #checkov:skip=CKV2_AWS_8:no AWS Backup plan for a one-time PoC; 1-day automated backups stay on
  #checkov:skip=CKV2_AWS_27:statement logging of benchmark writes would only add cost
  #checkov:skip=CKV_AWS_133:1-day retention is enough for a disposable PoC
  cluster_identifier                  = var.name
  engine                              = "aurora-postgresql"
  engine_version                      = var.engine_version
  global_cluster_identifier           = var.global_cluster_identifier
  database_name                       = var.is_secondary ? null : var.database_name
  master_username                     = var.is_secondary ? null : "cdc_admin"
  master_password_wo                  = var.is_secondary ? null : ephemeral.aws_secretsmanager_secret_version.admin[0].secret_string
  master_password_wo_version          = var.is_secondary ? null : 1
  db_subnet_group_name                = aws_db_subnet_group.this.name
  vpc_security_group_ids              = [var.security_group_id]
  db_cluster_parameter_group_name     = aws_rds_cluster_parameter_group.this.name
  storage_encrypted                   = true
  kms_key_id                          = var.kms_key_arn
  iam_database_authentication_enabled = true
  backup_retention_period             = 1
  skip_final_snapshot                 = true
  deletion_protection                 = false
  copy_tags_to_snapshot               = true
  apply_immediately                   = true
  enabled_cloudwatch_logs_exports     = ["postgresql"]

  serverlessv2_scaling_configuration {
    min_capacity = var.acu_min
    max_capacity = var.acu_max
  }

  depends_on = [aws_cloudwatch_log_group.postgresql]
}

resource "aws_rds_cluster_instance" "this" {
  #checkov:skip=CKV_AWS_118:enhanced monitoring adds log cost; Performance Insights is on
  count                           = var.instance_count
  identifier                      = "${var.name}-${count.index}"
  cluster_identifier              = aws_rds_cluster.this.id
  engine                          = aws_rds_cluster.this.engine
  engine_version                  = aws_rds_cluster.this.engine_version
  instance_class                  = "db.serverless"
  db_parameter_group_name         = aws_db_parameter_group.this.name
  publicly_accessible             = false
  promotion_tier                  = count.index
  auto_minor_version_upgrade      = true
  performance_insights_enabled    = true
  performance_insights_kms_key_id = var.kms_key_arn
  apply_immediately               = true
}

output "cluster_id" { value = aws_rds_cluster.this.id }
output "writer_endpoint" { value = aws_rds_cluster.this.endpoint }
output "instances_ready" { value = join(",", aws_rds_cluster_instance.this[*].id) }
