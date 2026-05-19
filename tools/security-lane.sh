#!/usr/bin/env bash
# Security lane evidence writer for workflows and local audit runs.
set -euo pipefail

REPORT_DIR="${JANKURAI_SECURITY_DIR:-target/jankurai/security}"
mkdir -p "$REPORT_DIR"

status=0
missing=()
checks=()

json_escape() {
    printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

record() {
    local name="$1"
    local result="$2"
    local artifact="$3"
    checks+=("{\"name\":\"$(json_escape "$name")\",\"status\":\"$(json_escape "$result")\",\"artifact\":\"$(json_escape "$artifact")\"}")
}

require_tool() {
    local tool="$1"
    if ! command -v "$tool" >/dev/null 2>&1; then
        missing+=("$tool")
        return 1
    fi
}

run_check() {
    local name="$1"
    local artifact="$2"
    shift 2
    if "$@" >"$artifact" 2>&1; then
        record "$name" "pass" "$artifact"
    else
        record "$name" "fail" "$artifact"
        status=1
    fi
}

run_advisory() {
    local name="$1"
    local artifact="$2"
    shift 2
    if "$@" >"$artifact" 2>&1; then
        record "$name" "pass" "$artifact"
    else
        record "$name" "advisory" "$artifact"
    fi
}

emit_evidence() {
    local missing_json="[]"
    local checks_json="[]"
    if ((${#missing[@]})); then
        missing_json="["
        local sep=""
        for item in "${missing[@]}"; do
            missing_json+="${sep}\"$(json_escape "$item")\""
            sep=","
        done
        missing_json+="]"
    fi
    if ((${#checks[@]})); then
        checks_json="["
        local sep=""
        for item in "${checks[@]}"; do
            checks_json+="${sep}${item}"
            sep=","
        done
        checks_json+="]"
    fi
    cat >"$REPORT_DIR/evidence.json" <<JSON
{
  "lane": "security",
  "status": ${status},
  "missing_tools": ${missing_json},
  "checks": ${checks_json}
}
JSON
}

for tool in actionlint gitleaks cargo; do
    require_tool "$tool" || status=1
done

if ((${#missing[@]})); then
    emit_evidence
    printf 'missing security tools: %s\n' "${missing[*]}" >&2
    exit "$status"
fi

run_check "actionlint" "$REPORT_DIR/actionlint.log" actionlint .github/workflows/*.yml
run_check "gitleaks" "$REPORT_DIR/gitleaks.log" gitleaks detect --source . --redact --report-format json --report-path "$REPORT_DIR/gitleaks.json"
run_advisory "cargo-audit" "$REPORT_DIR/cargo-audit.json" cargo audit --json
if command -v syft >/dev/null 2>&1; then
    run_check "syft" "$REPORT_DIR/sbom.json" syft . -o json
else
    record "syft" "advisory-missing" "$REPORT_DIR/sbom.json"
    printf '{"status":"missing","tool":"syft"}\n' >"$REPORT_DIR/sbom.json"
fi

emit_evidence
exit "$status"
