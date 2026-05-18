#!/usr/bin/env bash
set -euo pipefail

# Security lane implementation for Jankurai.
# Runs workflow, secret, dependency, and SBOM checks and generates receipts.

REPORT_DIR="target/jankurai/security"
mkdir -p "$REPORT_DIR"

STATUS=0
MISSING_TOOLS=()
RESULTS=()

record_result() {
  local name="$1"
  local status="$2"
  local artifact="$3"
  RESULTS+=("{\"name\":\"${name}\",\"status\":\"${status}\",\"artifact\":\"${artifact}\"}")
}

require_tool() {
  local tool="$1"
  if ! command -v "$tool" >/dev/null 2>&1; then
    MISSING_TOOLS+=("$tool")
    return 1
  fi
}

run_check() {
  local name="$1"
  local artifact="$2"
  shift 2

  echo "Running ${name}..." >&2
  if "$@" >"$artifact" 2>&1; then
    record_result "$name" "pass" "$artifact"
  else
    record_result "$name" "fail" "$artifact"
    STATUS=1
  fi
}

run_advisory_check() {
  local name="$1"
  local artifact="$2"
  shift 2

  echo "Running ${name}..." >&2
  if "$@" >"$artifact" 2>&1; then
    record_result "$name" "pass" "$artifact"
  else
    record_result "$name" "advisory" "$artifact"
  fi
}

run_gitleaks() {
  echo "Running gitleaks..." >&2
  if gitleaks detect --source . \
      --redact \
      --report-format json \
      --report-path "$REPORT_DIR/gitleaks.json" \
      >"$REPORT_DIR/gitleaks.log" 2>&1; then
    record_result "gitleaks" "pass" "$REPORT_DIR/gitleaks.json"
  else
    record_result "gitleaks" "fail" "$REPORT_DIR/gitleaks.json"
    STATUS=1
  fi
}

emit_evidence() {
  local missing_json="[]"
  if ((${#MISSING_TOOLS[@]} > 0)); then
    missing_json="["
    local separator=""
    for tool in "${MISSING_TOOLS[@]}"; do
      missing_json+="${separator}\"${tool}\""
      separator=","
    done
    missing_json+="]"
  fi

  local results_json="[]"
  if ((${#RESULTS[@]} > 0)); then
    results_json="["
    local separator=""
    for result in "${RESULTS[@]}"; do
      results_json+="${separator}${result}"
      separator=","
    done
    results_json+="]"
  fi

  cat > "$REPORT_DIR/evidence.json" <<JSON
{
  "lane": "security",
  "status": ${STATUS},
  "missing_tools": ${missing_json},
  "checks": ${results_json}
}
JSON
}

for tool in actionlint zizmor gitleaks cargo-audit syft; do
  require_tool "$tool" || STATUS=1
done

if ((${#MISSING_TOOLS[@]} > 0)); then
  printf 'Missing required security lane tools: %s\n' "${MISSING_TOOLS[*]}" >&2
  emit_evidence
  exit "$STATUS"
fi

run_check "actionlint" "$REPORT_DIR/actionlint.log" actionlint .github/workflows/*.yml
run_check "zizmor" "$REPORT_DIR/zizmor.json" zizmor --no-online-audits --min-severity medium --format json .github/workflows/*.yml
run_gitleaks
run_advisory_check "cargo-audit" "$REPORT_DIR/cargo-audit.json" cargo audit --json
run_check "syft" "$REPORT_DIR/sbom.json" syft . -o json

emit_evidence
exit "$STATUS"
