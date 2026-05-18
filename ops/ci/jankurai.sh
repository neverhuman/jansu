#!/usr/bin/env bash
# Jankurai audit lane — called by .github/workflows/jankurai.yml
set -euo pipefail

jankurai --version
just tool-adoption-evidence
mkdir -p target/jankurai/security
bash tools/security-lane.sh
jankurai security run . --out target/jankurai/security/evidence.json
jankurai audit . --mode advisory \
  --json target/jankurai/repo-score.json \
  --md target/jankurai/repo-score.md \
  --sarif target/jankurai/jankurai.sarif \
  --github-step-summary target/jankurai/summary.md \
  --repair-queue-jsonl target/jankurai/repair-queue.jsonl
just db-doctor
# Generate copy-code analysis
jankurai copy-code . --json target/jankurai/copy-code.json --md target/jankurai/copy-code.md
# Generate proofbind and proofmark evidence
mkdir -p target/jankurai/proofbind target/jankurai/proofmark
jankurai proofbind map . --out target/jankurai/proofbind/surface-witness.json --obligations-out target/jankurai/proofbind/obligations.json --md target/jankurai/proofbind/proofbind.md
jankurai proofmark rust . --obligations target/jankurai/proofbind/obligations.json --out target/jankurai/proofmark/proofmark-receipt.json --proof-receipt target/jankurai/proofmark/proof-receipt.json --md target/jankurai/proofmark/proofmark.md
