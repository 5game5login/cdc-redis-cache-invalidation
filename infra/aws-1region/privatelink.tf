# Node A :9094  <-  internal NLB  <-  endpoint service  <=PrivateLink=  VPC-B endpoint
# This is the only path between the two VPCs.

resource "aws_lb" "kafka" {
  #checkov:skip=CKV_AWS_150:PoC must tear down cleanly with terraform destroy
  #checkov:skip=CKV_AWS_91:NLB access logs cover only TLS listeners; Kafka terminates TLS itself
  name                             = "${local.name_a}-kafka"
  internal                         = true
  load_balancer_type               = "network"
  subnets                          = module.vpc_a.private_subnet_ids
  security_groups                  = [aws_security_group.nlb_a.id]
  enable_cross_zone_load_balancing = true # one target in one AZ, endpoint ENIs in two

  # Consumer IPs overlap VPC-A's own CIDR, so CIDR rules would be meaningless here.
  # Access control is allowed_principals + the endpoint's SG + mTLS + Kafka ACLs.
  enforce_security_group_inbound_rules_on_private_link_traffic = "off"
}

resource "aws_lb_target_group" "kafka" {
  name                 = "${local.name_a}-kafka"
  vpc_id               = module.vpc_a.vpc_id
  port                 = 9094
  protocol             = "TCP"
  target_type          = "instance"
  deregistration_delay = 5

  health_check {
    protocol = "TCP"
    interval = 10
  }
}

resource "aws_lb_target_group_attachment" "kafka" {
  target_group_arn = aws_lb_target_group.kafka.arn
  target_id        = module.node_a.instance_id
}

resource "aws_lb_listener" "kafka" {
  #checkov:skip=CKV_AWS_2:TCP passthrough on purpose; Kafka itself terminates mTLS
  #checkov:skip=CKV_AWS_103:TCP passthrough on purpose; Kafka itself terminates mTLS
  load_balancer_arn = aws_lb.kafka.arn
  port              = 9094
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.kafka.arn
  }
}

resource "aws_vpc_endpoint_service" "kafka" {
  #checkov:skip=CKV_AWS_123:auto-accept, but only this account's principals are allowed
  acceptance_required        = false
  network_load_balancer_arns = [aws_lb.kafka.arn]
  allowed_principals         = ["arn:${data.aws_partition.this.partition}:iam::${data.aws_caller_identity.this.account_id}:root"]
  supported_regions          = var.enable_cross_region ? [var.region, var.secondary_region] : null
  tags                       = { Name = "${local.name_a}-kafka" }
}

resource "aws_vpc_endpoint" "kafka" {
  provider           = aws.secondary
  vpc_id             = module.vpc_b.vpc_id
  vpc_endpoint_type  = "Interface"
  service_name       = aws_vpc_endpoint_service.kafka.service_name
  service_region     = var.enable_cross_region ? var.region : null
  subnet_ids         = module.vpc_b.private_subnet_ids
  security_group_ids = [aws_security_group.vpce_b.id]
  tags               = { Name = "${local.name_b}-kafka-a" }
  depends_on         = [aws_lb_listener.kafka]
}

# Stable name = what Kafka A advertises and what its certificate covers.
resource "aws_route53_zone" "b" {
  #checkov:skip=CKV2_AWS_38:DNSSEC signing does not apply to private zones
  #checkov:skip=CKV2_AWS_39:query logging does not apply to private zones
  provider      = aws.secondary
  name          = "cdc.internal"
  force_destroy = true

  vpc {
    vpc_id = module.vpc_b.vpc_id
  }
}

resource "aws_route53_record" "kafka_a" {
  #checkov:skip=CKV2_AWS_23:alias target is the PrivateLink endpoint
  provider = aws.secondary
  zone_id  = aws_route53_zone.b.zone_id
  name     = local.kafka_host
  type     = "A"

  alias {
    name                   = aws_vpc_endpoint.kafka.dns_entry[0].dns_name
    zone_id                = aws_vpc_endpoint.kafka.dns_entry[0].hosted_zone_id
    evaluate_target_health = false
  }
}
