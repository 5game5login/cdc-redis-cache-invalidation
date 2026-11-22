# Private bucket: node bundle (code only, no secrets) and benchmark results.
resource "aws_s3_bucket" "artifacts" {
  #checkov:skip=CKV_AWS_18:access logging of a throwaway code bucket adds nothing
  #checkov:skip=CKV_AWS_144:cross-Region replication is blocked in this project and not needed
  #checkov:skip=CKV_AWS_145:SSE-S3; the bucket holds no secrets
  #checkov:skip=CKV_AWS_21:bundle is rebuilt by Terraform; versions would only slow the destroy
  #checkov:skip=CKV2_AWS_61:bucket is destroyed with the PoC
  #checkov:skip=CKV2_AWS_62:no event consumers
  bucket_prefix = "${var.project}-artifacts-"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

data "aws_iam_policy_document" "artifacts" {
  statement {
    sid       = "TlsOnly"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "artifacts" {
  bucket     = aws_s3_bucket.artifacts.id
  policy     = data.aws_iam_policy_document.artifacts.json
  depends_on = [aws_s3_bucket_public_access_block.artifacts]
}

locals {
  repo = "${path.module}/../.."
  bundle = merge(
    { for f in fileset("${path.module}/node", "*") : f => "${path.module}/node/${f}" },
    { for f in fileset("${local.repo}/worker", "*.{py,lua}") : "worker/${f}" => "${local.repo}/worker/${f}" },
    { "register-connector.sh" = "${local.repo}/debezium/register-connector.sh" },
  )
}

data "archive_file" "bundle" {
  type             = "zip"
  output_file_mode = "0644" # readable by the non-root container user
  output_path      = "${path.module}/.build/bundle.zip"

  dynamic "source" {
    for_each = local.bundle
    content {
      filename = source.key
      # strip CR: these run on Linux whatever the operator's checkout uses
      content = replace(file(source.value), "\r", "")
    }
  }
}

resource "aws_s3_object" "bundle" {
  #checkov:skip=CKV_AWS_186:SSE-S3; the object holds no secrets
  bucket      = aws_s3_bucket.artifacts.id
  key         = "bundle/node.zip"
  source      = data.archive_file.bundle.output_path
  source_hash = data.archive_file.bundle.output_base64sha256
  depends_on  = [aws_s3_bucket_server_side_encryption_configuration.artifacts]
}
