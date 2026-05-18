#!/usr/bin/env bash
# Differential Kafka lab lane — called by .github/workflows/differential-kafka-lab.yml
set -euo pipefail

docker compose -f etc/differential/compose.kafka-4.2.yaml pull

CARGO_TARGET_DIR=/tmp/jansu-verify-phase04-contract \
  cargo test -p jansu-broker --test compatibility_contract --all-features -- --nocapture

JANSU_DIFFERENTIAL=1 \
JANSU_DIFF_ARTIFACT_DIR=target/differential \
CARGO_TARGET_DIR=/tmp/jansu-verify-phase04-differential \
  cargo test -p jansu-broker --test differential_lab --all-features -- --nocapture
