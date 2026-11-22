locals {
  tags = {
    Project   = var.project
    ManagedBy = "terraform"
    Purpose   = "one-time-poc"
  }
}

provider "aws" {
  region  = var.region
  profile = var.aws_profile

  default_tags {
    tags = local.tags
  }
}

# Every side-B resource uses this alias. It resolves to the same Region unless
# enable_cross_region is set, so the 2-Region path is the same code, not a copy.
provider "aws" {
  alias   = "secondary"
  region  = var.enable_cross_region ? var.secondary_region : var.region
  profile = var.aws_profile

  default_tags {
    tags = local.tags
  }
}
