#!/usr/bin/env bash
# Release lane — called by .github/workflows/release.yml
set -euo pipefail

just release
just docker-build
just test
bash tools/security-lane.sh
sha256sum target/release/jansu > target/release/jansu.sha256
cat target/release/jansu.sha256
