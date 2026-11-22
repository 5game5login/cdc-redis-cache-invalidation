variable "aws_profile" {
  type    = string
  default = "cdc-cache-poc"
}

variable "project" {
  type    = string
  default = "cdc-poc"
}

variable "region" {
  type    = string
  default = "us-east-2"
}

variable "enable_cross_region" {
  type        = bool
  default     = false
  description = <<-EOT
    Build side B in secondary_region with an Aurora Global Database and a cross-Region
    endpoint. Never applied, and its plan never completed: the account used for the PoC
    allows a single Region.
  EOT
}

variable "secondary_region" {
  type    = string
  default = "us-west-2"
}

variable "vpc_cidr" {
  type        = string
  default     = "10.10.0.0/16"
  description = "Used by BOTH VPCs on purpose: PrivateLink must work with overlapping CIDRs"
}

variable "aurora_engine_version" {
  type    = string
  default = "16.13"
}

variable "acu_min" {
  type    = number
  default = 0.5
}

variable "acu_max" {
  type    = number
  default = 4
}

variable "valkey_engine_version" {
  type    = string
  default = "8.2"
}

variable "cache_node_type" {
  type    = string
  default = "cache.t4g.micro"
}

variable "instance_type_a" {
  type    = string
  default = "t4g.large"
}

variable "instance_type_b" {
  type    = string
  default = "t4g.medium"
}

variable "log_retention_days" {
  type    = number
  default = 7
}
