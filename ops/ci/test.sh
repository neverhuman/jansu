#!/usr/bin/env bash
# Main CI test lane — called by .github/workflows/ci.yml
set -euo pipefail

. "$(dirname "$0")/lib.sh"

ci_test
