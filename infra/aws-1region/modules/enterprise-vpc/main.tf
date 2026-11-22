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
variable "cidr" { type = string }
variable "kms_key_arn" { type = string }
variable "log_retention_days" { type = number }

data "aws_region" "this" {}
data "aws_caller_identity" "this" {}

locals {
  # Pinned, not looked up: the set must not change when AWS adds a zone
  azs = [for z in ["a", "b"] : "${data.aws_region.this.region}${z}"]
}

resource "aws_vpc" "this" {
  cidr_block           = var.cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = var.name }
}

# Locked: no rules, so nothing can use it by accident.
resource "aws_default_security_group" "this" {
  vpc_id = aws_vpc.this.id
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.this.id
  availability_zone = local.azs[count.index]
  cidr_block        = cidrsubnet(var.cidr, 4, count.index)
  tags              = { Name = "${var.name}-private-${count.index}" }
}

# Holds only the NAT gateway. No instance is ever placed here.
resource "aws_subnet" "public" {
  vpc_id            = aws_vpc.this.id
  availability_zone = local.azs[0]
  cidr_block        = cidrsubnet(var.cidr, 8, 240)
  tags              = { Name = "${var.name}-public-nat" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = var.name }
}

resource "aws_eip" "nat" {
  #checkov:skip=CKV2_AWS_19:attached to the NAT gateway
  domain     = "vpc"
  tags       = { Name = "${var.name}-nat" }
  depends_on = [aws_internet_gateway.this]
}

# Single-AZ NAT: documented cost trade-off (egress only, not in the data path).
resource "aws_nat_gateway" "this" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public.id
  tags          = { Name = var.name }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name}-public" }
}

resource "aws_route" "public_default" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name}-private" }
}

resource "aws_route" "private_default" {
  route_table_id         = aws_route_table.private.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.this.id
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# Free; keeps S3 traffic (bundle, OS packages) off the NAT gateway.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${data.aws_region.this.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]
  tags              = { Name = "${var.name}-s3" }
}

# ---- flow logs
resource "aws_cloudwatch_log_group" "flow" {
  #checkov:skip=CKV_AWS_338:7-day retention is the plan's choice for a one-time PoC
  name              = "/${var.name}/vpc-flow"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn
}

data "aws_iam_policy_document" "flow_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["vpc-flow-logs.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.this.account_id]
    }
  }
}

data "aws_iam_policy_document" "flow" {
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
    resources = ["${aws_cloudwatch_log_group.flow.arn}:*"]
  }
}

resource "aws_iam_role" "flow" {
  name               = "${var.name}-flow-logs"
  assume_role_policy = data.aws_iam_policy_document.flow_assume.json
}

resource "aws_iam_role_policy" "flow" {
  name   = "write"
  role   = aws_iam_role.flow.id
  policy = data.aws_iam_policy_document.flow.json
}

resource "aws_flow_log" "this" {
  vpc_id                   = aws_vpc.this.id
  traffic_type             = "ALL"
  log_destination          = aws_cloudwatch_log_group.flow.arn
  iam_role_arn             = aws_iam_role.flow.arn
  max_aggregation_interval = 600
}

output "vpc_id" { value = aws_vpc.this.id }
output "private_subnet_ids" { value = aws_subnet.private[*].id }

# Nodes boot only after this is known, so they never start without egress.
output "egress_ready" {
  value = join(",", concat(
    [aws_route.private_default.id, aws_route.public_default.id, aws_route_table_association.public.id],
    aws_route_table_association.private[*].id,
  ))
}
