#!/bin/bash -l
# vllm_run.sh – sbatch submission script for vLLM via Apptainer/Singularity.
#
# All dynamic variables are injected as environment variables by the caller
# (HpcConnect.submit_vllm/3). Never edit paths in this file; pass them in.
#
# Required env vars:
#   HPC_MODELS_DIR   – vault path where models live (e.g. /home/vault/yourgroup/yourusername)
#   HPC_WORK_DIR     – work/cache path            (e.g. /home/hpc/yourgroup/yourusername/.cache/hpc_connect)
#   HPC_SIF_PATH     – absolute path to the .sif  (e.g. $HPC_WORK_DIR/singularity_images/vllm.sif)
#   VLLM_MODEL       – HuggingFace repo-id         (e.g. meta-llama/Llama-3.2-1B-Instruct)
#   VLLM_PORT        – port to bind               (default: 8000)
#   VLLM_TP          – tensor-parallel size        (default: 1)
#   VLLM_GPU_MEM     – GPU memory utilisation      (default: 0.90)
#   VLLM_MAX_LEN     – max-model-len              (default: 8192)
#
# SBATCH directives are supplied via --export / --wrap or the generated wrapper
# written by HpcConnect. They are listed here as comments only; the actual
# #SBATCH lines are injected by the Elixir caller so every job can override them.
#SBATCH --job-name=hpc_connect_vllm
#SBATCH --ntasks=1

set -Eeuo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
VLLM_PORT="${VLLM_PORT:-8000}"
VLLM_TP="${VLLM_TP:-1}"
VLLM_GPU_MEM="${VLLM_GPU_MEM:-0.90}"
VLLM_MAX_LEN="${VLLM_MAX_LEN:-8192}"
HF_TOKEN="${HF_TOKEN:-}"

# ---------------------------------------------------------------------------
# Validate required vars
# ---------------------------------------------------------------------------
: "${HPC_MODELS_DIR:?HPC_MODELS_DIR is required}"
: "${HPC_WORK_DIR:?HPC_WORK_DIR is required}"
: "${HPC_SIF_PATH:?HPC_SIF_PATH is required}"
: "${VLLM_MODEL:?VLLM_MODEL is required}"

LOGS_DIR="${HPC_WORK_DIR}/logs"
CACHE_DIR="${HPC_WORK_DIR}/hf_cache"
mkdir -p "$LOGS_DIR" "$CACHE_DIR"

RUNTIME_LOG="${LOGS_DIR}/vllm_runtime_${SLURM_JOB_ID:-manual}.log"
exec > >(tee -a "$RUNTIME_LOG") 2>&1

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
ts()  { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] [INFO] $*"; }
err() { echo "[$(ts)] [ERROR] $*" >&2; }
die() { err "$*"; exit 1; }

on_error() { local ec=$?; err "Script failed at line $1 (exit $ec)"; exit "$ec"; }
trap 'on_error $LINENO' ERR

# ---------------------------------------------------------------------------
# Sanity checks
# ---------------------------------------------------------------------------
[[ -f "$HPC_SIF_PATH" ]] || die "SIF image not found: $HPC_SIF_PATH"
command -v apptainer >/dev/null 2>&1 || die "apptainer not found in PATH"

# Model directory: vault/<repo-id with / replaced by -->
MODEL_SUBDIR="${VLLM_MODEL//\//-}"
MODEL_LOCAL_DIR="${HPC_MODELS_DIR}/${MODEL_SUBDIR}"

[[ -d "$MODEL_LOCAL_DIR" ]] || die "Model directory not found: $MODEL_LOCAL_DIR  (run download_model first)"
[[ -n "$(ls -A "$MODEL_LOCAL_DIR" 2>/dev/null)" ]] || die "Model directory is empty: $MODEL_LOCAL_DIR"

# ---------------------------------------------------------------------------
# Info
# ---------------------------------------------------------------------------
log "=== vLLM startup ==="
log "Hostname       : $(hostname)"
log "Job ID         : ${SLURM_JOB_ID:-manual}"
log "SIF            : $HPC_SIF_PATH"
log "Model          : $VLLM_MODEL"
log "Model dir      : $MODEL_LOCAL_DIR"
log "Port           : $VLLM_PORT"
log "Tensor parallel: $VLLM_TP"
log "GPU mem util   : $VLLM_GPU_MEM"
log "Max model len  : $VLLM_MAX_LEN"
log "Cache dir      : $CACHE_DIR"

# ---------------------------------------------------------------------------
# Apptainer env passthrough
# ---------------------------------------------------------------------------
export APPTAINER_ENV_HF_HOME=/hf_cache
export APPTAINER_ENV_TRANSFORMERS_CACHE=/hf_cache
export APPTAINER_ENV_VLLM_NO_USAGE_STATS=1
export APPTAINER_ENV_PYTHONUNBUFFERED=1

if [[ -n "$HF_TOKEN" ]]; then
  export APPTAINER_ENV_HF_TOKEN="$HF_TOKEN"
  log "HF_TOKEN will be forwarded into the container"
fi

# ---------------------------------------------------------------------------
# Wait-for-port helper (pure bash, no Python dependency)
# ---------------------------------------------------------------------------
wait_for_port() {
  local host="${1:-127.0.0.1}" port="${2:-8000}" tries="${3:-90}"
  local i=1
  while (( i <= tries )); do
    # Use /dev/tcp – available in bash on Linux
    if (echo > /dev/tcp/"$host"/"$port") 2>/dev/null; then
      log "vLLM is listening on ${host}:${port}"
      return 0
    fi
    log "Waiting for vLLM on ${host}:${port} (${i}/${tries})…"
    sleep 5
    (( i++ ))
  done
  log "WARNING: port ${port} did not open within expected time (continuing)"
  return 0
}

# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------
log "Launching Apptainer container…"

apptainer run --nv \
  --bind "${CACHE_DIR}:/hf_cache" \
  --bind "${HPC_MODELS_DIR}:/models" \
  "$HPC_SIF_PATH" \
  --host 0.0.0.0 \
  --port "$VLLM_PORT" \
  --model "/models/${MODEL_SUBDIR}" \
  --tensor-parallel-size "$VLLM_TP" \
  --gpu-memory-utilization "$VLLM_GPU_MEM" \
  --max-model-len "$VLLM_MAX_LEN" &

VLLM_PID=$!
log "vLLM PID: $VLLM_PID"

wait_for_port 127.0.0.1 "$VLLM_PORT" 90

wait "$VLLM_PID"
log "vLLM process exited — job done at $(date)"
