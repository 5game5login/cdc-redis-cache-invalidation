# 2-Region path (enable_cross_region=true only). Never applied.
resource "aws_rds_global_cluster" "this" {
  count                     = var.enable_cross_region ? 1 : 0
  global_cluster_identifier = "${var.project}-global"
  engine                    = "aurora-postgresql"
  engine_version            = var.aurora_engine_version
  database_name             = local.db_name
  storage_encrypted         = true
  force_destroy             = true
}

resource "aws_security_group" "aurora_b" {
  #checkov:skip=CKV2_AWS_5:attached to module.aurora_b
  count       = var.enable_cross_region ? 1 : 0
  provider    = aws.secondary
  name        = "${local.name_b}-aurora"
  description = "Aurora secondary: no clients until promoted"
  vpc_id      = module.vpc_b.vpc_id
}

module "aurora_b" {
  count                     = var.enable_cross_region ? 1 : 0
  source                    = "./modules/aurora-cluster"
  providers                 = { aws = aws.secondary }
  name                      = local.name_b
  subnet_ids                = module.vpc_b.private_subnet_ids
  security_group_id         = aws_security_group.aurora_b[0].id
  kms_key_arn               = module.secrets_b.kms_key_arn
  engine_version            = var.aurora_engine_version
  database_name             = local.db_name
  acu_min                   = var.acu_min
  acu_max                   = var.acu_max
  instance_count            = 1
  is_secondary              = true
  global_cluster_identifier = aws_rds_global_cluster.this[0].id
  log_retention_days        = var.log_retention_days
  depends_on                = [module.aurora_a]
}
