#!/usr/bin/env bash
# Checks the alert rules (deploy/alerting) and runs their unit tests with
# promtool, from the Prometheus image the stack runs (task 5.6).
set -euo pipefail
cd "$(dirname "$0")/.."
image=$(awk '/image: prom\/prometheus:/ { print $2; exit }' compose.yaml)
docker run --rm --entrypoint promtool -v "$PWD/deploy/alerting:/rules:ro" -w /rules "$image" check rules holdfast.rules.yml
docker run --rm --entrypoint promtool -v "$PWD/deploy/alerting:/rules:ro" -w /rules "$image" test rules tests.yml
