# Private EC2 node (AL2023 arm64). SSM only: no key pair, no public IP, no inbound SSH.
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
variable "side" { type = string }
variable "instance_type" { type = string }
variable "subnet_id" { type = string }
variable "security_group_id" { type = string }
variable "kms_key_arn" { type = string }
variable "secret_arns" { type = list(string) }
variable "bucket" { type = string }
variable "bundle_key" { type = string }
variable "bucket_region" { type = string }
variable "log_retention_days" { type = number }

variable "node_conf" {
  type        = map(string)
  description = "Non-secret settings for /opt/cdc/node.conf"
}

variable "boot_after" {
  type        = string
  description = "Opaque value; the instance is created only once it is known (egress, databases ...)"
}

data "aws_region" "this" {}
data "aws_partition" "this" {}

data "aws_ssm_parameter" "ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

resource "aws_cloudwatch_log_group" "this" {
  #checkov:skip=CKV_AWS_338:7-day retention is the plan's choice for a one-time PoC
  name              = "/${var.name}/node"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "node" {
  statement {
    sid       = "OwnSecretsOnly"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = var.secret_arns
  }
  statement {
    sid       = "DecryptOwnSecrets"
    actions   = ["kms:Decrypt"]
    resources = [var.kms_key_arn]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["secretsmanager.${data.aws_region.this.region}.amazonaws.com"]
    }
  }
  statement {
    sid       = "ReadBundle"
    actions   = ["s3:GetObject"]
    resources = ["arn:${data.aws_partition.this.partition}:s3:::${var.bucket}/${var.bundle_key}"]
  }
  statement {
    sid       = "BenchmarkRuns"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["arn:${data.aws_partition.this.partition}:s3:::${var.bucket}/runs/*"]
  }
  statement {
    sid       = "ShipLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
    resources = ["${aws_cloudwatch_log_group.this.arn}:*"]
  }
}

resource "aws_iam_role" "this" {
  name               = "${var.name}-node"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

resource "aws_iam_role_policy" "this" {
  name   = "node"
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.node.json
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.this.name
  policy_arn = "arn:${data.aws_partition.this.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "this" {
  name = "${var.name}-node"
  role = aws_iam_role.this.name
}

resource "aws_instance" "this" {
  #checkov:skip=CKV_AWS_126:1-minute metrics are not needed for a short PoC
  ami                         = data.aws_ssm_parameter.ami.insecure_value
  instance_type               = var.instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [var.security_group_id]
  iam_instance_profile        = aws_iam_instance_profile.this.name
  associate_public_ip_address = false
  ebs_optimized               = true

  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 1 # containers cannot reach the role credentials
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = 30
    encrypted   = true
    kms_key_id  = var.kms_key_arn
  }

  credit_specification {
    # "standard" starts with ~0 credits and pinned node B at its 20% baseline, which
    # failed Kafka's health check (2026-11-21). Surplus is ~$0.04 per vCPU-hour.
    cpu_credits = "unlimited"
  }

  user_data = templatefile("${path.module}/user-data.sh.tftpl", {
    side          = var.side
    region        = data.aws_region.this.region
    bucket        = var.bucket
    bucket_region = var.bucket_region
    bundle_key    = var.bundle_key
    log_group     = aws_cloudwatch_log_group.this.name
    node_conf     = var.node_conf
  })

  tags = {
    Name      = "${var.name}-node"
    BootAfter = substr(sha1(var.boot_after), 0, 8)
  }

  lifecycle {
    ignore_changes = [ami, user_data]
  }

  depends_on = [aws_iam_role_policy.this, aws_iam_role_policy_attachment.ssm]
}

output "instance_id" { value = aws_instance.this.id }
output "log_group" { value = aws_cloudwatch_log_group.this.name }
