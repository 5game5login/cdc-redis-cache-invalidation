# Separate from the PoC stack on purpose: the budget must outlive `terraform destroy`,
# because AWS cost data lags usage by hours. Budgets notify; they do not stop spend.
terraform {
  required_version = ">= 1.11"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.73"
    }
  }
  backend "local" {}
}

variable "aws_profile" {
  type    = string
  default = "cdc-cache-poc"
}

variable "limit_usd" {
  type    = number
  default = 50
}

variable "alert_email" {
  type        = string
  description = "Address that receives the alerts (no default: pass it explicitly)"
}

provider "aws" {
  region  = "us-east-2"
  profile = var.aws_profile
}

resource "aws_budgets_budget" "monthly" {
  name         = "cdc-poc-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = [
      { type = "ACTUAL", pct = 40 },
      { type = "ACTUAL", pct = 80 },
      { type = "FORECASTED", pct = 80 },
      { type = "FORECASTED", pct = 100 },
    ]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value.pct
      threshold_type             = "PERCENTAGE"
      notification_type          = notification.value.type
      subscriber_email_addresses = [var.alert_email]
    }
  }
}
