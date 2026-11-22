# Throwaway CA for the 9094 listener: one server identity (A), one client identity
# (B's MirrorMaker, principal User:CN=mm2-b). 14-day validity. These private keys
# are in Terraform state (providers cannot sign ephemerally), hence the state path
# rule in versions.tf.
resource "tls_private_key" "kafka" {
  for_each  = toset(["ca", "server", "client"])
  algorithm = "RSA"
  rsa_bits  = 2048
}

resource "tls_self_signed_cert" "ca" {
  private_key_pem       = tls_private_key.kafka["ca"].private_key_pem
  is_ca_certificate     = true
  validity_period_hours = 336
  allowed_uses          = ["cert_signing", "crl_signing", "digital_signature"]

  subject {
    common_name = "${var.project}-ca"
  }
}

resource "tls_cert_request" "leaf" {
  for_each        = { server = local.kafka_host, client = "mm2-b" }
  private_key_pem = tls_private_key.kafka[each.key].private_key_pem
  dns_names       = [each.value]

  subject {
    common_name = each.value
  }
}

resource "tls_locally_signed_cert" "leaf" {
  for_each              = tls_cert_request.leaf
  cert_request_pem      = each.value.cert_request_pem
  ca_private_key_pem    = tls_private_key.kafka["ca"].private_key_pem
  ca_cert_pem           = tls_self_signed_cert.ca.cert_pem
  validity_period_hours = 336
  allowed_uses          = ["digital_signature", "key_encipherment", each.key == "server" ? "server_auth" : "client_auth"]
}

locals {
  # Kafka PEM keystores need PKCS#8 keys.
  kafka_tls_a = jsonencode({
    ca   = tls_self_signed_cert.ca.cert_pem
    key  = tls_private_key.kafka["server"].private_key_pem_pkcs8
    cert = tls_locally_signed_cert.leaf["server"].cert_pem
  })
  kafka_tls_b = jsonencode({
    ca   = tls_self_signed_cert.ca.cert_pem
    key  = tls_private_key.kafka["client"].private_key_pem_pkcs8
    cert = tls_locally_signed_cert.leaf["client"].cert_pem
  })
}
