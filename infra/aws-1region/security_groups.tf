# Defined here (not in modules) so rules can reference each other without cycles.
resource "aws_security_group" "node_a" {
  #checkov:skip=CKV2_AWS_5:false positive; attached through module.node_a
  name        = "${local.name_a}-node"
  description = "cdc-node-A: Kafka, Debezium, worker"
  vpc_id      = module.vpc_a.vpc_id
}

resource "aws_security_group" "aurora_a" {
  name        = "${local.name_a}-aurora"
  description = "Aurora: 5432 from node A only"
  vpc_id      = module.vpc_a.vpc_id
}

resource "aws_security_group" "cache_a" {
  name        = "${local.name_a}-cache"
  description = "Valkey A: 6379 from node A only"
  vpc_id      = module.vpc_a.vpc_id
}

resource "aws_security_group" "nlb_a" {
  name        = "${local.name_a}-nlb"
  description = "NLB: PrivateLink traffic to node A 9094 only"
  vpc_id      = module.vpc_a.vpc_id
}

resource "aws_security_group" "node_b" {
  provider    = aws.secondary
  name        = "${local.name_b}-node"
  description = "cdc-node-B: Kafka, MirrorMaker 2, worker"
  vpc_id      = module.vpc_b.vpc_id
}

resource "aws_security_group" "cache_b" {
  provider    = aws.secondary
  name        = "${local.name_b}-cache"
  description = "Valkey B: 6379 from node B only"
  vpc_id      = module.vpc_b.vpc_id
}

resource "aws_security_group" "vpce_b" {
  provider    = aws.secondary
  name        = "${local.name_b}-vpce"
  description = "PrivateLink endpoint: 9094 from node B only"
  vpc_id      = module.vpc_b.vpc_id
}

# ---- side A
resource "aws_vpc_security_group_egress_rule" "node_a_https" {
  security_group_id = aws_security_group.node_a.id
  description       = "SSM, Secrets Manager, Logs, image pulls (via NAT)"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}

resource "aws_vpc_security_group_egress_rule" "node_a_aurora" {
  security_group_id            = aws_security_group.node_a.id
  description                  = "Aurora"
  referenced_security_group_id = aws_security_group.aurora_a.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}

resource "aws_vpc_security_group_egress_rule" "node_a_cache" {
  security_group_id            = aws_security_group.node_a.id
  description                  = "Valkey A"
  referenced_security_group_id = aws_security_group.cache_a.id
  ip_protocol                  = "tcp"
  from_port                    = 6379
  to_port                      = 6379
}

resource "aws_vpc_security_group_ingress_rule" "node_a_kafka" {
  security_group_id            = aws_security_group.node_a.id
  description                  = "Kafka mTLS from the NLB only"
  referenced_security_group_id = aws_security_group.nlb_a.id
  ip_protocol                  = "tcp"
  from_port                    = 9094
  to_port                      = 9094
}

resource "aws_vpc_security_group_ingress_rule" "aurora_a" {
  security_group_id            = aws_security_group.aurora_a.id
  description                  = "PostgreSQL from node A"
  referenced_security_group_id = aws_security_group.node_a.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}

resource "aws_vpc_security_group_ingress_rule" "cache_a" {
  security_group_id            = aws_security_group.cache_a.id
  description                  = "Valkey from node A"
  referenced_security_group_id = aws_security_group.node_a.id
  ip_protocol                  = "tcp"
  from_port                    = 6379
  to_port                      = 6379
}

# No ingress rule: nothing inside VPC-A may use the NLB. PrivateLink traffic is
# exempt (see privatelink.tf) and gated by the consumer's endpoint SG plus mTLS.
resource "aws_vpc_security_group_egress_rule" "nlb_a" {
  security_group_id            = aws_security_group.nlb_a.id
  description                  = "Kafka on node A"
  referenced_security_group_id = aws_security_group.node_a.id
  ip_protocol                  = "tcp"
  from_port                    = 9094
  to_port                      = 9094
}

# ---- side B
resource "aws_vpc_security_group_egress_rule" "node_b_https" {
  provider          = aws.secondary
  security_group_id = aws_security_group.node_b.id
  description       = "SSM, Secrets Manager, Logs, image pulls (via NAT)"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}

resource "aws_vpc_security_group_egress_rule" "node_b_cache" {
  provider                     = aws.secondary
  security_group_id            = aws_security_group.node_b.id
  description                  = "Valkey B"
  referenced_security_group_id = aws_security_group.cache_b.id
  ip_protocol                  = "tcp"
  from_port                    = 6379
  to_port                      = 6379
}

resource "aws_vpc_security_group_egress_rule" "node_b_vpce" {
  provider                     = aws.secondary
  security_group_id            = aws_security_group.node_b.id
  description                  = "Kafka A through the PrivateLink endpoint"
  referenced_security_group_id = aws_security_group.vpce_b.id
  ip_protocol                  = "tcp"
  from_port                    = 9094
  to_port                      = 9094
}

resource "aws_vpc_security_group_ingress_rule" "cache_b" {
  provider                     = aws.secondary
  security_group_id            = aws_security_group.cache_b.id
  description                  = "Valkey from node B"
  referenced_security_group_id = aws_security_group.node_b.id
  ip_protocol                  = "tcp"
  from_port                    = 6379
  to_port                      = 6379
}

resource "aws_vpc_security_group_ingress_rule" "vpce_b" {
  provider                     = aws.secondary
  security_group_id            = aws_security_group.vpce_b.id
  description                  = "Kafka A from node B"
  referenced_security_group_id = aws_security_group.node_b.id
  ip_protocol                  = "tcp"
  from_port                    = 9094
  to_port                      = 9094
}

locals { # nodes boot only after their rules exist
  node_a_rules_ready = join(",", [
    aws_vpc_security_group_egress_rule.node_a_https.id,
    aws_vpc_security_group_egress_rule.node_a_aurora.id,
    aws_vpc_security_group_egress_rule.node_a_cache.id,
    aws_vpc_security_group_ingress_rule.aurora_a.id,
    aws_vpc_security_group_ingress_rule.cache_a.id,
  ])
  node_b_rules_ready = join(",", [
    aws_vpc_security_group_egress_rule.node_b_https.id,
    aws_vpc_security_group_egress_rule.node_b_cache.id,
    aws_vpc_security_group_egress_rule.node_b_vpce.id,
    aws_vpc_security_group_ingress_rule.cache_b.id,
    aws_vpc_security_group_ingress_rule.vpce_b.id,
  ])
}
