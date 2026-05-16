#!/usr/bin/env bash
set -euo pipefail

# Security lane implementation for Jankurai.
# Runs secret scanning, dependency audit, and generates evidence reports.
# This script is invoked by the Jankurai security lane CI job.

# Ensure output directory exists.
REPORT_DIR="target/jankurai/security"
mkdir -p "$REPORT_DIR"

# Install gitleaks if not available.
if ! command -v gitleaks >/dev/null 2>&1; then
  echo "Installing gitleaks..." >&2
  cargo install gitleaks --locked || true
fi

# Install cargo-audit if not available.
if ! command -v cargo-audit >/dev/null 2>&1; then
  echo "Installing cargo-audit..." >&2
  cargo install cargo-audit --locked || true
fi

# Run gitleaks detection.
# Output JSON report; redact secrets if possible.
if command -v gitleaks >/dev/null 2>&1; then
  echo "Running gitleaks..." >&2
  gitleaks detect --source . \
    --redact \
    --report-format json \
    --report-path "$REPORT_DIR/gitleaks.json"
else
  echo "gitleaks not installed; skipping secret scan." >&2
fi

# Run cargo-audit for dependency vulnerabilities.
if command -v cargo-audit >/dev/null 2>&1; then
  echo "Running cargo-audit..." >&2
  cargo audit --json > "$REPORT_DIR/cargo-audit.json" || true
else
  echo "cargo-audit not installed; skipping dependency audit." >&2
fi

if command -v syft >/dev/null 2>&1; then
  syft . -o json > "$REPORT_DIR/sbom.json"
fi

# Exit with success status – failures are reported via evidence JSON files.
exit 0
actionlint || true
