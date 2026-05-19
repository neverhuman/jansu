#!/usr/bin/env bash
# Jankurai audit lane called by .github/workflows/jankurai.yml.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

jankurai --version
just tool-adoption-evidence
jankurai audit . \
    --mode advisory \
    --json agent/repo-score.json \
    --md agent/repo-score.md \
    --sarif target/jankurai/jankurai.sarif \
    --github-step-summary target/jankurai/summary.md \
    --repair-queue-jsonl target/jankurai/repair-queue.jsonl \
    --no-score-history
cp agent/repo-score.json target/jankurai/repo-score.json
cp agent/repo-score.md target/jankurai/repo-score.md
JANKURAI_REUSE_SCORE=1 bash tools/checks/jankurai-gate.sh
