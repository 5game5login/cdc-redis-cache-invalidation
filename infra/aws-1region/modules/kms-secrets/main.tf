# One CMK per side plus that side's secrets. Generated values are ephemeral and
# written with write-only arguments, so they never enter Terraform state.
terraform {
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.73"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.9"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.13"
    }
  }
}

variable "name" { type = string }
variable "create_db_secrets" { type = bool }

variable "kafka_tls_json" {
  type        = string
  sensitive   = true
  description = "JSON {ca, key, cert} for this side's Kafka TLS identity"
}

data "aws_caller_identity" "this" {}
data "aws_partition" "this" {}
data "aws_region" "this" {}

data "aws_iam_policy_document" "key" {
  #checkov:skip=CKV_AWS_109:a key policy's Resource "*" means this key; root delegation lets IAM govern access
  #checkov:skip=CKV_AWS_111:a key policy's Resource "*" means this key
  #checkov:skip=CKV_AWS_356:a key policy's Resource "*" means this key
  statement {
    sid       = "AccountDelegation"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.this.partition}:iam::${data.aws_caller_identity.this.account_id}:root"]
    }
  }
  statement {
    sid       = "CloudWatchLogs"
    actions   = ["kms:Encrypt*", "kms:Decrypt*", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:Describe*"]
    resources = ["*"]
    principals {
      type        = "Service"
      identifiers = ["logs.${data.aws_region.this.region}.amazonaws.com"]
    }
    condition {
      test     = "ArnLike"
      variable = "kms:EncryptionContext:aws:logs:arn"
      values   = ["arn:${data.aws_partition.this.partition}:logs:${data.aws_region.this.region}:${data.aws_caller_identity.this.account_id}:log-group:*"]
    }
  }
}

resource "aws_kms_key" "this" {
  description             = "${var.name}: Aurora, ElastiCache, EBS, Secrets Manager, Logs"
  enable_key_rotation     = true
  deletion_window_in_days = 7
  policy                  = data.aws_iam_policy_document.key.json
}

resource "aws_kms_alias" "this" {
  name          = "alias/${var.name}"
  target_key_id = aws_kms_key.this.key_id
}

# ---- Redis AUTH
ephemeral "random_password" "redis" {
  length  = 48
  special = false
}

resource "aws_secretsmanager_secret" "redis" {
  #checkov:skip=CKV2_AWS_57:one-time PoC destroyed after the run; rotation would need a Lambda
  name                    = "${var.name}/redis-auth"
  kms_key_id              = aws_kms_key.this.arn
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "redis" {
  secret_id                = aws_secretsmanager_secret.redis.id
  secret_string_wo         = ephemeral.random_password.redis.result
  secret_string_wo_version = 1
}

# ---- Kafka TLS identity
resource "aws_secretsmanager_secret" "kafka_tls" {
  #checkov:skip=CKV2_AWS_57:certificates live 14 days and the PoC is destroyed after the run
  name                    = "${var.name}/kafka-tls"
  kms_key_id              = aws_kms_key.this.arn
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "kafka_tls" {
  secret_id                = aws_secretsmanager_secret.kafka_tls.id
  secret_string_wo         = var.kafka_tls_json
  secret_string_wo_version = 1
}

# ---- database (primary side only)
ephemeral "random_password" "db" {
  for_each = toset(["admin", "debezium", "cdc_monitor", "cdc_app"])
  length   = 40
  special  = false
}

resource "aws_secretsmanager_secret" "db_admin" {
  #checkov:skip=CKV2_AWS_57:one-time PoC destroyed after the run
  count                   = var.create_db_secrets ? 1 : 0
  name                    = "${var.name}/db-admin"
  kms_key_id              = aws_kms_key.this.arn
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "db_admin" {
  count                    = var.create_db_secrets ? 1 : 0
  secret_id                = aws_secretsmanager_secret.db_admin[0].id
  secret_string_wo         = ephemeral.random_password.db["admin"].result
  secret_string_wo_version = 1
}

resource "aws_secretsmanager_secret" "db_roles" {
  #checkov:skip=CKV2_AWS_57:one-time PoC destroyed after the run
  count                   = var.create_db_secrets ? 1 : 0
  name                    = "${var.name}/db-roles"
  kms_key_id              = aws_kms_key.this.arn
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "db_roles" {
  count     = var.create_db_secrets ? 1 : 0
  secret_id = aws_secretsmanager_secret.db_roles[0].id
  secret_string_wo = jsonencode({
    debezium    = ephemeral.random_password.db["debezium"].result
    cdc_monitor = ephemeral.random_password.db["cdc_monitor"].result
    cdc_app     = ephemeral.random_password.db["cdc_app"].result
  })
  secret_string_wo_version = 1
}

# Secrets Manager is eventually consistent: reading a version right after writing it
# can fail with "couldn't find resource" (seen on the first apply).
resource "time_sleep" "secrets_settle" {
  create_duration = "20s"
  depends_on      = [aws_secretsmanager_secret_version.redis, aws_secretsmanager_secret_version.db_admin]
}

output "kms_key_arn" { value = aws_kms_key.this.arn }

# From the *version* resources: known only once a value exists, so consumers that
# read the secret back always find it populated.
output "kafka_tls_secret_arn" { value = aws_secretsmanager_secret_version.kafka_tls.secret_arn }

output "redis_secret_arn" {
  value      = aws_secretsmanager_secret_version.redis.secret_arn
  depends_on = [time_sleep.secrets_settle]
}

output "db_admin_secret_arn" {
  value      = one(aws_secretsmanager_secret_version.db_admin[*].secret_arn)
  depends_on = [time_sleep.secrets_settle]
}

output "db_roles_secret_arn" { value = one(aws_secretsmanager_secret_version.db_roles[*].secret_arn) }
