data "aws_caller_identity" "this" {}
data "aws_partition" "this" {}

locals {
  name_a     = "${var.project}-a"
  name_b     = "${var.project}-b"
  db_name    = "inventory"
  kafka_host = "kafka-a.cdc.internal"
}

# ---- side A (primary) ------------------------------------------------------
module "secrets_a" {
  source            = "./modules/kms-secrets"
  name              = local.name_a
  create_db_secrets = true
  kafka_tls_json    = local.kafka_tls_a
}

module "vpc_a" {
  source             = "./modules/enterprise-vpc"
  name               = local.name_a
  cidr               = var.vpc_cidr
  kms_key_arn        = module.secrets_a.kms_key_arn
  log_retention_days = var.log_retention_days
}

module "aurora_a" {
  source                    = "./modules/aurora-cluster"
  name                      = local.name_a
  subnet_ids                = module.vpc_a.private_subnet_ids
  security_group_id         = aws_security_group.aurora_a.id
  kms_key_arn               = module.secrets_a.kms_key_arn
  engine_version            = var.aurora_engine_version
  database_name             = local.db_name
  acu_min                   = var.acu_min
  acu_max                   = var.acu_max
  admin_secret_arn          = module.secrets_a.db_admin_secret_arn
  global_cluster_identifier = one(aws_rds_global_cluster.this[*].id)
  log_retention_days        = var.log_retention_days
}

module "cache_a" {
  source            = "./modules/elasticache"
  name              = local.name_a
  subnet_ids        = module.vpc_a.private_subnet_ids
  security_group_id = aws_security_group.cache_a.id
  kms_key_arn       = module.secrets_a.kms_key_arn
  auth_secret_arn   = module.secrets_a.redis_secret_arn
  node_count        = 2
  node_type         = var.cache_node_type
  engine_version    = var.valkey_engine_version
}

module "node_a" {
  source             = "./modules/cdc-node"
  name               = local.name_a
  side               = "a"
  instance_type      = var.instance_type_a
  subnet_id          = module.vpc_a.private_subnet_ids[0]
  security_group_id  = aws_security_group.node_a.id
  kms_key_arn        = module.secrets_a.kms_key_arn
  bucket             = aws_s3_bucket.artifacts.id
  bucket_region      = var.region
  bundle_key         = aws_s3_object.bundle.key
  log_retention_days = var.log_retention_days
  secret_arns = [
    module.secrets_a.redis_secret_arn,
    module.secrets_a.kafka_tls_secret_arn,
    module.secrets_a.db_admin_secret_arn,
    module.secrets_a.db_roles_secret_arn,
  ]
  node_conf = {
    KAFKA_PRIVATELINK_HOST = local.kafka_host
    REDIS_HOST             = module.cache_a.endpoint
    DB_HOST                = module.aurora_a.writer_endpoint
    DB_NAME                = local.db_name
    REDIS_SECRET_ID        = module.secrets_a.redis_secret_arn
    KAFKA_TLS_SECRET_ID    = module.secrets_a.kafka_tls_secret_arn
    DB_SECRET_ID           = module.secrets_a.db_roles_secret_arn
    DB_ADMIN_SECRET_ID     = module.secrets_a.db_admin_secret_arn
  }
  boot_after = "${module.vpc_a.egress_ready}|${module.aurora_a.instances_ready}|${local.node_a_rules_ready}"
}

# ---- side B (secondary): no route to A except the PrivateLink endpoint ------
module "secrets_b" {
  source            = "./modules/kms-secrets"
  providers         = { aws = aws.secondary }
  name              = local.name_b
  create_db_secrets = false
  kafka_tls_json    = local.kafka_tls_b
}

module "vpc_b" {
  source             = "./modules/enterprise-vpc"
  providers          = { aws = aws.secondary }
  name               = local.name_b
  cidr               = var.vpc_cidr
  kms_key_arn        = module.secrets_b.kms_key_arn
  log_retention_days = var.log_retention_days
}

module "cache_b" {
  source            = "./modules/elasticache"
  providers         = { aws = aws.secondary }
  name              = local.name_b
  subnet_ids        = module.vpc_b.private_subnet_ids
  security_group_id = aws_security_group.cache_b.id
  kms_key_arn       = module.secrets_b.kms_key_arn
  auth_secret_arn   = module.secrets_b.redis_secret_arn
  node_count        = 1
  node_type         = var.cache_node_type
  engine_version    = var.valkey_engine_version
}

module "node_b" {
  source             = "./modules/cdc-node"
  providers          = { aws = aws.secondary }
  name               = local.name_b
  side               = "b"
  instance_type      = var.instance_type_b
  subnet_id          = module.vpc_b.private_subnet_ids[0]
  security_group_id  = aws_security_group.node_b.id
  kms_key_arn        = module.secrets_b.kms_key_arn
  bucket             = aws_s3_bucket.artifacts.id
  bucket_region      = var.region
  bundle_key         = aws_s3_object.bundle.key
  log_retention_days = var.log_retention_days
  secret_arns        = [module.secrets_b.redis_secret_arn, module.secrets_b.kafka_tls_secret_arn]
  node_conf = {
    KAFKA_PRIVATELINK_HOST = local.kafka_host
    REDIS_HOST             = module.cache_b.endpoint
    REDIS_SECRET_ID        = module.secrets_b.redis_secret_arn
    KAFKA_TLS_SECRET_ID    = module.secrets_b.kafka_tls_secret_arn
  }
  boot_after = "${module.vpc_b.egress_ready}|${aws_route53_record.kafka_a.id}|${local.node_b_rules_ready}"
}
