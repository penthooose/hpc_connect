#!/bin/bash -l
# build_sif.sh – builds a Singularity/Apptainer SIF image on the cluster.
#
# Designed to be submitted via sbatch (CPU-only job, no GPU needed) or run
# interactively on a login node.
#
# Required env vars:
#   HPC_WORK_DIR  – base work/cache path
#   DEF_NAME      – stem name of the .def file (e.g. "vllm")
#                   Looks for:  $HPC_WORK_DIR/singularity_def_files/<DEF_NAME>.def
#                   Writes to:  $HPC_WORK_DIR/singularity_images/<DEF_NAME>.sif
#
# Optional:
#   FORCE_REBUILD    – set to "1" to rebuild even if .sif already exists
#   APPTAINER_TMPDIR – override apptainer's default tmp/cache dir (normally ~/.apptainer)
#                      Unset = let apptainer manage its own cache; use `apptainer cache clean`
#
# SBATCH directives are injected by the Elixir caller as needed; comments only:
#SBATCH --job-name=hpc_connect_build_sif
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4

set -Eeuo pipefail

: "${HPC_WORK_DIR:?HPC_WORK_DIR is required}"
: "${DEF_NAME:?DEF_NAME is required}"

DEF_FILES_DIR="${HPC_WORK_DIR}/singularity_def_files"
IMAGES_DIR="${HPC_WORK_DIR}/singularity_images"
FORCE_REBUILD="${FORCE_REBUILD:-0}"

mkdir -p "$IMAGES_DIR"

DEF_FILE="${DEF_FILES_DIR}/${DEF_NAME}.def"
SIF_FILE="${IMAGES_DIR}/${DEF_NAME}.sif"

ts()  { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] [INFO] $*"; }
err() { echo "[$(ts)] [ERROR] $*" >&2; }
die() { err "$*"; exit 1; }

on_error() { local ec=$?; err "Build failed at line $1 (exit $ec)"; exit "$ec"; }
trap 'on_error $LINENO' ERR

[[ -f "$DEF_FILE" ]] || die "Definition file not found: $DEF_FILE"
command -v apptainer >/dev/null 2>&1 || die "apptainer not found in PATH"

if [[ -f "$SIF_FILE" && "$FORCE_REBUILD" != "1" ]]; then
  log "SIF already exists: $SIF_FILE (set FORCE_REBUILD=1 to rebuild)"
  echo "$SIF_FILE"
  exit 0
fi

log "=== Apptainer build ==="
log "Hostname : $(hostname)"
log "Def file : $DEF_FILE"
log "SIF out  : $SIF_FILE"
[[ -n "${APPTAINER_TMPDIR:-}" ]] && log "Tmp dir  : $APPTAINER_TMPDIR (override)" || log "Tmp dir  : apptainer default (~/.apptainer)"

# --ignore-fakeroot-command avoids the user-namespace setgroups issue on
# login nodes where /proc/self/setgroups writes are blocked by kernel policy.
apptainer build --force --ignore-fakeroot-command "$SIF_FILE" "$DEF_FILE"

log "Build complete: $SIF_FILE"
log "Size: $(du -sh "$SIF_FILE" | cut -f1)"

# Print the path so callers can capture it
echo "$SIF_FILE"
