#!/bin/bash -l
# start_atp_benchmark_runner.sh - host-side single-node ATP benchmark launcher.
#
# This runs on the allocated cluster node and dispatches the existing prover
# .sif images via `apptainer exec`. No master benchmark-runner container is
# required for this direct-prover mode.

set -Eeuo pipefail

ATP_BENCHMARK_RUNNER_SINGLE_NODE_MODE="${ATP_BENCHMARK_RUNNER_SINGLE_NODE_MODE:-parallel}"
ATP_BENCHMARK_RUNNER_MAX_PARALLEL="${ATP_BENCHMARK_RUNNER_MAX_PARALLEL:-1}"
ATP_BENCHMARK_RUNNER_LOG_DIR="${ATP_BENCHMARK_RUNNER_LOG_DIR:-${HPC_WORK_DIR:-$HOME/.cache/hpc_connect}/logs}"

: "${HPC_WORK_DIR:?HPC_WORK_DIR is required}"
: "${ATP_BENCHMARK_RUNNER_TASKS_FILE:?ATP_BENCHMARK_RUNNER_TASKS_FILE is required}"
: "${ATP_BENCHMARK_RUNNER_RESULTS_DIR:?ATP_BENCHMARK_RUNNER_RESULTS_DIR is required}"
: "${ATP_BENCHMARK_RUNNER_TIMEOUT_SECONDS:?ATP_BENCHMARK_RUNNER_TIMEOUT_SECONDS is required}"

mkdir -p "$ATP_BENCHMARK_RUNNER_RESULTS_DIR" "$ATP_BENCHMARK_RUNNER_LOG_DIR"

RUNTIME_LOG="${ATP_BENCHMARK_RUNNER_LOG_DIR}/atp_benchmark_runner_${SLURM_JOB_ID:-manual}.log"
exec > >(tee -a "$RUNTIME_LOG") 2>&1

ts()  { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] [INFO] $*"; }
err() { echo "[$(ts)] [ERROR] $*" >&2; }
die() { err "$*"; exit 1; }

on_error() { local ec=$?; err "Script failed at line $1 (exit $ec)"; exit "$ec"; }
trap 'on_error $LINENO' ERR

command -v apptainer >/dev/null 2>&1 || die "apptainer not found in PATH"

export OMP_NUM_THREADS=1
export OMP_PLACES=cores
export OMP_PROC_BIND=true
export SRUN_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK:-1}"
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1
export LIGHTGBM_NUM_THREADS=1
export NTHREAD=1
export GOMP_SPINCOUNT=0
export MKL_CBWR=AUTO

run_task() {
  local prover="$1"
  local problem_path="$2"
  local problem_id="$3"
  local command_b64="$4"
  local result_dir="$ATP_BENCHMARK_RUNNER_RESULTS_DIR/$prover"
  local out_file="$result_dir/${problem_id}.out"
  local meta_file="$result_dir/${problem_id}.meta.json"
  local resource_file="$result_dir/${problem_id}.resources.txt"
  local command
  local start_epoch_ms
  local end_epoch_ms
  local wall_time_ms
  local exit_status=0
  local memory_kb=null

  mkdir -p "$result_dir"
  command=$(printf '%s' "$command_b64" | base64 -d)

  export PROBLEM_PATH="$problem_path"
  export OUT_FILE="$out_file"
  export META_FILE="$meta_file"
  export RESOURCE_FILE="$resource_file"
  export TIMEOUT_SECONDS="$ATP_BENCHMARK_RUNNER_TIMEOUT_SECONDS"

  start_epoch_ms=$(date +%s%3N)

  if command -v /usr/bin/time >/dev/null 2>&1; then
    /usr/bin/time -f 'elapsed_seconds=%e\nmax_rss_kb=%M' -o "$resource_file" \
      timeout --preserve-status "${TIMEOUT_SECONDS}s" bash -lc "$command" > "$out_file" 2>&1 || exit_status=$?
  else
    timeout --preserve-status "${TIMEOUT_SECONDS}s" bash -lc "$command" > "$out_file" 2>&1 || exit_status=$?
  fi

  end_epoch_ms=$(date +%s%3N)
  wall_time_ms=$((end_epoch_ms - start_epoch_ms))

  if [ -f "$resource_file" ]; then
    parsed_memory=$(awk -F= '$1 == "max_rss_kb" {print $2; exit}' "$resource_file")
    if echo "$parsed_memory" | grep -Eq '^[0-9]+$'; then
      memory_kb="$parsed_memory"
    fi
  fi

  if ! grep -qi "SZS status" "$out_file"; then
    if [ "$exit_status" = "124" ] || [ "$exit_status" = "137" ]; then
      echo "% SZS status Timeout for ${problem_id}" >> "$out_file"
    else
      echo "% SZS status GaveUp for ${problem_id}" >> "$out_file"
    fi
  fi

  printf '{"problem_id":"%s","prover":"%s","exit_status":%s,"wall_time_ms":%s,"memory_kb":%s,"output_path":"%s","resource_path":"%s"}\n' \
    "$problem_id" "$prover" "$exit_status" "$wall_time_ms" "$memory_kb" "$out_file" "$resource_file" > "$meta_file"
}

log "=== ATP benchmark runner startup ==="
log "Hostname          : $(hostname)"
log "Job ID            : ${SLURM_JOB_ID:-manual}"
log "Tasks file        : $ATP_BENCHMARK_RUNNER_TASKS_FILE"
log "Results dir       : $ATP_BENCHMARK_RUNNER_RESULTS_DIR"
log "Timeout seconds   : $ATP_BENCHMARK_RUNNER_TIMEOUT_SECONDS"
log "Single node mode  : $ATP_BENCHMARK_RUNNER_SINGLE_NODE_MODE"
log "Max parallel      : $ATP_BENCHMARK_RUNNER_MAX_PARALLEL"

case "$ATP_BENCHMARK_RUNNER_SINGLE_NODE_MODE" in
  sequential)
    while IFS=$'\t' read -r prover problem_path problem_id command_b64; do
      [ -z "$prover" ] && continue
      run_task "$prover" "$problem_path" "$problem_id" "$command_b64"
    done < "$ATP_BENCHMARK_RUNNER_TASKS_FILE"
    ;;
  parallel|*)
    while IFS=$'\t' read -r prover problem_path problem_id command_b64; do
      [ -z "$prover" ] && continue
      run_task "$prover" "$problem_path" "$problem_id" "$command_b64" &

      while [ "$(jobs -pr | wc -l)" -ge "$ATP_BENCHMARK_RUNNER_MAX_PARALLEL" ]; do
        sleep 1
      done
    done < "$ATP_BENCHMARK_RUNNER_TASKS_FILE"

    wait
    ;;
esac

log "ATP benchmark runner job finished"