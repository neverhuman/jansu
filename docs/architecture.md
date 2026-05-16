# Architecture Index

This repo keeps architecture, boundaries, and proof guidance in local docs so agents do not have to infer intent from code alone.

Read first:
- `docs/architecture/README.md`
- `docs/agent-native-standard.md`
- `docs/testing.md`
- `docs/release.md`
- `docs/AGENTS.md`

Boundary notes:
- Keep product truth in Rust, SQL, and generated contracts.
- Route durable cross-cutting guidance through `agent/` manifests and docs.
- Prefer local proof lanes and receipts over handwritten claims.
