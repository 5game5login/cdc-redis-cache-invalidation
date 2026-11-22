locals {
  region_b = var.enable_cross_region ? var.secondary_region : var.region
}

# Consumed by scripts/cdc_remote.py and scripts/test_failover.sh
output "nodes" {
  value = {
    region_a   = var.region
    region_b   = local.region_b
    instance_a = module.node_a.instance_id
    instance_b = module.node_b.instance_id
    bucket     = aws_s3_bucket.artifacts.id
    cluster_id = module.aurora_a.cluster_id
  }
}

output "privatelink" {
  value = {
    service  = aws_vpc_endpoint_service.kafka.service_name
    endpoint = aws_vpc_endpoint.kafka.dns_entry[0].dns_name
    name     = local.kafka_host
  }
}

output "log_groups" {
  value = { a = module.node_a.log_group, b = module.node_b.log_group }
}

output "ssm_sessions" {
  value = {
    a = "aws ssm start-session --target ${module.node_a.instance_id} --region ${var.region} --profile ${var.aws_profile}"
    b = "aws ssm start-session --target ${module.node_b.instance_id} --region ${local.region_b} --profile ${var.aws_profile}"
  }
}
