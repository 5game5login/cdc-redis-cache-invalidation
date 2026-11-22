#!/usr/bin/env bash
# Terraform wrapper: keeps state, plugins and plans outside the project folder
# (which may be cloud-synced; state holds the Kafka TLS keys).
#   scripts/tf.sh init | plan | apply | destroy | output -json nodes
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA="${CDC_TF_HOME:-${LOCALAPPDATA:-$HOME/.local/share}/cdc-poc-terraform}"
if command -v cygpath > /dev/null; then DATA="$(cygpath -m "${DATA}")"; ROOT="$(cygpath -m "${ROOT}")"; fi
mkdir -p "${DATA}"
export TF_DATA_DIR="${DATA}/data" TF_IN_AUTOMATION=1
TF="${TERRAFORM_BIN:-terraform}"
if ! command -v "${TF}" > /dev/null; then # a winget install may need a new shell to reach PATH
  TF="$(ls "${LOCALAPPDATA}"/Microsoft/WinGet/Packages/Hashicorp.Terraform_*/terraform.exe | head -1)"
fi
if [[ "${1:-}" == init ]]; then
  shift
  exec "${TF}" -chdir="${ROOT}" init -backend-config="path=${DATA}/terraform.tfstate" "$@"
fi
exec "${TF}" -chdir="${ROOT}" "$@"
