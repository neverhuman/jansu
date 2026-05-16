#!/usr/bin/env bash
# Jankurai audit lane — called by .github/workflows/jankurai.yml
set -euo pipefail

cargo install jankurai --locked
jankurai --version
jankurai audit . --mode advisory \
  --json target/jankurai/repo-score.json \
  --md target/jankurai/repo-score.md \
  --sarif target/jankurai/jankurai.sarif \
  --github-step-summary target/jankurai/summary.md \
  --repair-queue-jsonl target/jankurai/repair-queue.jsonl
just db-doctor
just tool-adoption-evidence
