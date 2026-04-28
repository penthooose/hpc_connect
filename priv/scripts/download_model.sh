#!/usr/bin/env bash
# Download a Hugging Face model snapshot to a local directory.
# Usage: download_model.sh --repo <repo_id> --target <dir> [--revision <rev>] [--token-env <VAR>]
set -euo pipefail

ensure_python3() {
	# helper: return 0 only for python >= 3.8
	has_modern_python() {
		command -v python3 >/dev/null 2>&1 || return 1
		python3 - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 8) else 1)
PY
	}

	if has_modern_python; then
		return 0
	fi

	# On FAU/NHR clusters, force a modern python via modules.
	if command -v module >/dev/null 2>&1; then
		module load python/3.12-conda >/dev/null 2>&1 || true
	elif [[ -f /etc/profile.d/modules.sh ]]; then
		# Some non-interactive shells need explicit module init.
		# shellcheck source=/dev/null
		source /etc/profile.d/modules.sh >/dev/null 2>&1 || true
		if command -v module >/dev/null 2>&1; then
			module load python/3.12-conda >/dev/null 2>&1 || true
		fi
	fi

	if ! has_modern_python; then
		echo "python3 >= 3.8 is required (current python is too old or missing)." >&2
		echo "Try manually: module load python/3.12-conda" >&2
		exit 1
	fi
}

ensure_huggingface_hub() {
	if python3 -c "import huggingface_hub" >/dev/null 2>&1; then
		return 0
	fi

	# Keep this lightweight and user-local by default.
	if ! python3 -m pip install --user --upgrade --no-warn-script-location huggingface_hub >/dev/null; then
		echo "Failed to install huggingface_hub automatically." >&2
		echo "Try manually: python3 -m pip install --user --upgrade huggingface_hub" >&2
		exit 1
	fi
}

REPO=""
TARGET=""
REVISION=""
TOKEN_ENV=""

while [[ $# -gt 0 ]]; do
	case "$1" in
		--repo)       REPO="$2";       shift 2 ;;
		--target)     TARGET="$2";     shift 2 ;;
		--revision)   REVISION="$2";   shift 2 ;;
		--token-env)  TOKEN_ENV="$2";  shift 2 ;;
		*) echo "Unknown argument: $1" >&2; exit 1 ;;
	esac
done

[[ -n "$REPO"   ]] || { echo "--repo is required"   >&2; exit 1; }
[[ -n "$TARGET" ]] || { echo "--target is required" >&2; exit 1; }

TOKEN=""
if [[ -n "$TOKEN_ENV" ]]; then
	TOKEN="${!TOKEN_ENV:-}"
fi

mkdir -p "$TARGET"

ensure_python3
ensure_huggingface_hub

python3 -W ignore::UserWarning - <<PY
import os, sys
from huggingface_hub import snapshot_download

kwargs = dict(
    repo_id="${REPO}",
    local_dir="${TARGET}",
)

token = "${TOKEN}" or None
if token:
    kwargs["token"] = token

revision = "${REVISION}" or None
if revision:
    kwargs["revision"] = revision

snapshot_download(**kwargs)
print("Downloaded to: ${TARGET}")
PY
