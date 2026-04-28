#!/usr/bin/env bash
set -euo pipefail

CACHE_DIR="${1:?cache dir required}"

cat <<EOF
export HF_HOME="${CACHE_DIR}"
export HUGGINGFACE_HUB_CACHE="${CACHE_DIR}"
export TRANSFORMERS_CACHE="${CACHE_DIR}"
EOF
