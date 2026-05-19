Agent: Codex

Prompt: Confirm the latest `neverhuman/jankurai` install, run one more worker pass, and move the Jankurai audit forward quickly.

Phase Or Audit Item: AUDIT-019 cross-phase Jankurai score recovery.

Files Read:
- AGENTS.md
- agent/repo-score.json
- agent/repo-score.md
- agent/owner-map.json
- agent/test-map.json
- agent/boundaries.toml
- agent/tool-adoption.toml
- .github/workflows/jankurai.yml
- compose.yaml
- docs/testing.md
- docs/security/authz-matrix.md
- docs/security/input-boundary.md
- jansu-embedded/src/lib.rs
- jansu-embedded/examples/single_binary.rs
- jansu-schema/src/avro/arrow.rs
- jansu-schema/src/json/arrow.rs
- jansu-schema/src/proto/arrow.rs
- tools/checks/jankurai-gate.sh

Files Changed:
- .github/workflows/jankurai.yml
- agent/boundaries.toml
- agent/owner-map.json
- agent/test-map.json
- compose.yaml
- docs/testing.md
- docs/security/authz-matrix.md
- docs/security/input-boundary.md
- jansu-embedded/src/lib.rs
- jansu-embedded/examples/single_binary.rs
- jansu-schema/src/avro/arrow.rs
- jansu-schema/src/json/arrow.rs
- jansu-schema/src/proto/arrow.rs
- tools/checks/jankurai-gate.sh

Tests Added: None.

Verification Commands:
- `rtk which jankurai`
- `rtk jankurai --version`
- `rtk git ls-remote https://github.com/neverhuman/jankurai HEAD`
- `rtk sed -n '1,120p' /home/ubuntu/.cargo/.crates.toml`
- `rtk jankurai doctor --fail-on critical`
- `rtk bash -n tools/checks/jankurai-gate.sh ops/ci/jankurai.sh ops/ci/jankurai-audit.sh scripts/ci-local.sh`
- `rtk docker compose --env-file example.env config --quiet`
- `rtk cargo fmt --all --check`
- `rtk git diff --check`
- `rtk just score`

Outcome:
- Confirmed the installed binary is `jankurai 1.5.0` from `git+https://github.com/neverhuman/jankurai.git?rev=49e80b9404902bc646023e5a174bbc1e17e0a0ad`.
- Confirmed `49e80b9404902bc646023e5a174bbc1e17e0a0ad` is the current `neverhuman/jankurai` HEAD and tag `v1.5.0`.
- Ran a third 10-worker read-only pass with current score context and narrower prompts.
- Removed the Docker cap and the owner/test-map findings.
- Moved refreshed score from `score=64 raw=70 caps=10 findings=143` to `score=64 raw=72 caps=9 findings=135`.

Residual Risks:
- `rtk just score` still fails because caps/findings remain: HLT-001 wording/placeholders, HLT-030 SQL destructive-delete findings, and singleton authz/input/release/observability/boundary proof findings.
- Jankurai tool-adoption dimension still reports `ci_evidence=0` and `artifact_verified=0` even after local evidence generation, so the remaining HLT-016 likely needs native Jankurai CI/adoption command evidence rather than local placeholder artifacts.
- Input-boundary and streaming-boundary findings move to the next matching file after each small cleanup, so clearing those caps probably requires broader manifest/tool configuration rather than one-off wording edits.

Next Recommended Action:
- Attack HLT-001 by replacing actual `todo!`/`unimplemented!` runtime branches with typed errors in schema/perf code, then rerun `rtk just score`.
- For HLT-030, rewrite SQL delete assets to direct `DELETE ... USING`/`EXISTS` predicates or add Jankurai-native DB proof receipts if the scanner supports them.
