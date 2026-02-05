#!/usr/bin/env bash
set -euo pipefail

# Run the internal backend on a fixed local port, and expose the frontend on $PORT.
export AVAILABILITY_BACKEND_URL="${AVAILABILITY_BACKEND_URL:-http://127.0.0.1:5050}"

uvicorn services.gateway.app:app --host 0.0.0.0 --port 5050 &
GATEWAY_PID=$!

trap 'kill ${GATEWAY_PID} 2>/dev/null || true' SIGINT SIGTERM

uvicorn apps.web_flights.app:app --host 0.0.0.0 --port "${PORT:-8000}"
