#!/usr/bin/env bash
# Find a free TCP port in a given range.
# Usage: find_free_port.sh <min_port> <max_port>
set -euo pipefail

MIN_PORT="${1:?min_port is required}"
MAX_PORT="${2:?max_port is required}"

python3 - <<PY
import socket

for port in range(${MIN_PORT}, ${MAX_PORT} + 1):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("0.0.0.0", port))
            print(port)
            raise SystemExit(0)
        except OSError:
            continue

raise SystemExit("No free port found in range ${MIN_PORT}-${MAX_PORT}")
PY
