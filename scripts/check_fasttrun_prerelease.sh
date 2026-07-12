#!/usr/bin/env bash
# Полный обязательный cassert-набор перед релизом и для nightly.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export FASTTRUN_BRIN_LEVEL=full

exec "$SCRIPT_DIR/check_cassert_allversions.sh" "$@"
