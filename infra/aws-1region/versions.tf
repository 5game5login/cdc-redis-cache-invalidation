terraform {
  required_version = ">= 1.11" # write-only arguments

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.73"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.9"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.4"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.13"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
  }

  # Path supplied at init by scripts/tf.sh. State holds the Kafka TLS keys, so it must
  # stay out of version control and cloud-synced folders.
  backend "local" {}
}
