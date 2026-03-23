#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://localhost:8080}"
ROUNDS="${2:-20}"

echo "Load test on $BASE_URL for $ROUNDS rounds"

for ((i=1; i<=ROUNDS; i++)); do
  curl -s "$BASE_URL/cpu?loops=2500000" > /dev/null
  curl -s "$BASE_URL/io?mb=16" > /dev/null
  curl -s "$BASE_URL/mem?mb=64" > /dev/null
  curl -s "$BASE_URL/mixed?workers=4&loops=500000" > /dev/null
  if (( i % 5 == 0 )); then
    echo "round $i complete"
  fi
done

echo "done"
