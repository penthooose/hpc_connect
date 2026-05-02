#!/usr/bin/env bash
set -euo pipefail

MODEL_PATH=""
HOST="0.0.0.0"
PORT="8000"
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --model)
      MODEL_PATH="$2"
      shift 2
      ;;
    --host)
      HOST="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --)
      shift
      EXTRA_ARGS=("$@")
      break
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "${MODEL_PATH}" && -n "${VLLM_MODEL:-}" && -n "${HPC_MODELS_DIR:-}" ]]; then
  MODEL_SUBDIR="${VLLM_MODEL//\//-}"
  MODEL_PATH="${HPC_MODELS_DIR}/${MODEL_SUBDIR}"
fi

if [[ -n "${APP_ARGS:-}" ]]; then
  read -r -a APP_ARG_ARRAY <<< "$APP_ARGS"
  EXTRA_ARGS+=("${APP_ARG_ARRAY[@]}")
fi

if [[ -z "${MODEL_PATH}" ]]; then
  echo "--model is required or set VLLM_MODEL/HPC_MODELS_DIR" >&2
  exit 1
fi

cmd=(python -m vllm.entrypoints.openai.api_server \
  --model "${MODEL_PATH}" \
  --host "${HOST}" \
  --port "${PORT}")

if [[ -n "${VLLM_TP:-}" ]]; then
  cmd+=(--tensor-parallel-size "${VLLM_TP}")
fi

if [[ -n "${VLLM_GPU_MEM:-}" ]]; then
  cmd+=(--gpu-memory-utilization "${VLLM_GPU_MEM}")
fi

if [[ -n "${VLLM_MAX_LEN:-}" ]]; then
  cmd+=(--max-model-len "${VLLM_MAX_LEN}")
fi

cmd+=("${EXTRA_ARGS[@]}")

exec "${cmd[@]}"
