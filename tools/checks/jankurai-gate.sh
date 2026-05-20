#!/usr/bin/env bash
# Canonical Jankurai audit gate for local checks and CI.
#
# Produces the repository score under agent/ and fails when the current
# Jankurai audit reports any hard cap, or any finding that is not already
# recorded in the baseline. The baseline may only hold documented auditor
# false-positives — see AUDIT.md (AUDIT-019) for the justification of each.
# Caps are severe and are never baselined: any cap fails the gate.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

BASELINE_FILE="${JANKURAI_GATE_BASELINE:-agent/jankurai-gate-baseline.json}"
SCORE_JSON="${JANKURAI_SCORE_JSON:-agent/repo-score.json}"
SCORE_MD="${JANKURAI_SCORE_MD:-agent/repo-score.md}"
TARGET_SCORE_JSON="${JANKURAI_TARGET_SCORE_JSON:-target/jankurai/repo-score.json}"
TARGET_SCORE_MD="${JANKURAI_TARGET_SCORE_MD:-target/jankurai/repo-score.md}"
SARIF="${JANKURAI_SARIF:-target/jankurai/jankurai.sarif}"
SUMMARY_MD="${JANKURAI_STEP_SUMMARY:-target/jankurai/summary.md}"
REPAIR_QUEUE="${JANKURAI_REPAIR_QUEUE:-target/jankurai/repair-queue.jsonl}"

require_tool() {
    local tool="$1"
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "JANKURAI GATE FAILED - missing required tool: $tool" >&2
        exit 127
    fi
}

require_score_shape() {
    local file="$1"
    jq -e '
      (.caps_applied // empty | type == "array")
      and (.findings // empty | type == "array")
    ' "$file" >/dev/null || {
        echo "JANKURAI GATE FAILED - $file must contain caps_applied[] and findings[]" >&2
        exit 1
    }
}

require_tool jankurai
require_tool jq

# AUDIT-019 anti-masking guard: refuse to score if any authored .rs file has
# been hidden from the audit via excluded_paths or generated-zones.
bash "$REPO_ROOT/tools/checks/no-mask.sh"

if [ ! -f "$BASELINE_FILE" ]; then
    echo "JANKURAI GATE FAILED - missing baseline: $BASELINE_FILE" >&2
    exit 1
fi

mkdir -p \
    "$(dirname "$SCORE_JSON")" \
    "$(dirname "$SCORE_MD")" \
    "$(dirname "$TARGET_SCORE_JSON")" \
    "$(dirname "$TARGET_SCORE_MD")" \
    "$(dirname "$SARIF")" \
    "$(dirname "$SUMMARY_MD")" \
    "$(dirname "$REPAIR_QUEUE")"

if [ "${JANKURAI_REUSE_SCORE:-0}" != "1" ]; then
    if command -v just >/dev/null 2>&1 && just --summary | tr ' ' '\n' | grep -qx tool-adoption-evidence; then
        just tool-adoption-evidence
    fi

    jankurai audit . \
        --mode advisory \
        --json "$SCORE_JSON" \
        --md "$SCORE_MD" \
        --sarif "$SARIF" \
        --github-step-summary "$SUMMARY_MD" \
        --repair-queue-jsonl "$REPAIR_QUEUE" \
        --no-score-history

    cp "$SCORE_JSON" "$TARGET_SCORE_JSON"
    cp "$SCORE_MD" "$TARGET_SCORE_MD"
fi

require_score_shape "$BASELINE_FILE"
require_score_shape "$SCORE_JSON"

score="$(jq -r '.score // 0' "$SCORE_JSON")"
raw_score="$(jq -r '.raw_score // 0' "$SCORE_JSON")"
current_caps="$(jq -r '(.caps_applied // []) | length' "$SCORE_JSON")"
current_findings="$(jq -r '(.findings // []) | length' "$SCORE_JSON")"
baseline_caps="$(jq -r '(.caps_applied // []) | length' "$BASELINE_FILE")"
baseline_findings="$(jq -r '(.findings // []) | length' "$BASELINE_FILE")"

new_caps="$(
    jq -n \
        --slurpfile current "$SCORE_JSON" \
        --slurpfile baseline "$BASELINE_FILE" \
        '($baseline[0].caps_applied // []) as $base
        | (($current[0].caps_applied // [])
          | map(select(. as $cap | ($base | index($cap) | not)))
          | length)'
)"

new_findings="$(
    jq -n \
        --slurpfile current "$SCORE_JSON" \
        --slurpfile baseline "$BASELINE_FILE" \
        'def key($finding):
          [
            ($finding.fingerprint // ""),
            ($finding.check_id // $finding.rule_id // $finding.rule // ""),
            ($finding.path // ""),
            ($finding.severity // "")
          ] | @json;
        (($baseline[0].findings // []) | map(key(.))) as $base
        | (($current[0].findings // [])
          | map(select(key(.) as $k | ($base | index($k) | not)))
          | length)'
)"

echo "jankurai-gate: score=$score raw=$raw_score caps=$current_caps/$baseline_caps new_caps=$new_caps findings=$current_findings/$baseline_findings new_findings=$new_findings"

# Caps are severe and are never baselined: any cap fails the gate.
if [ "$current_caps" -gt 0 ]; then
    echo ""
    echo "JANKURAI GATE FAILED - audit reports $current_caps hard cap(s); caps are never accepted"
    echo ""
    echo "Score artifacts:"
    echo "  $SCORE_JSON"
    echo "  $SCORE_MD"
    echo ""
    echo "Current caps:"
    jq '(.caps_applied // [])' "$SCORE_JSON"
    exit 1
fi

# Any finding not present in the baseline is a regression and fails the gate.
# The baseline holds only documented auditor false-positives (see AUDIT.md).
if [ "$new_findings" -gt 0 ]; then
    echo ""
    echo "JANKURAI GATE FAILED - $new_findings finding(s) not present in the baseline"
    echo ""
    echo "Score artifacts:"
    echo "  $SCORE_JSON"
    echo "  $SCORE_MD"
    echo ""
    echo "Current findings:"
    jq '(.findings // [])' "$SCORE_JSON"
    echo ""
    echo "Baseline (documented false-positives only): $BASELINE_FILE"
    echo "A new finding must be fixed honestly, not added to the baseline,"
    echo "unless it is a proven auditor false-positive documented in AUDIT.md."
    exit 1
fi

if [ "$current_findings" -gt 0 ]; then
    echo "JANKURAI GATE PASS - 0 caps; $current_findings baselined false-positive finding(s), 0 new"
else
    echo "JANKURAI GATE PASS - current caps and findings are zero"
fi
