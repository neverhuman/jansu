# Streaming Boundary

This document captures the temporary streaming-runtime exception while
AUDIT-019 is open.

## Current Exception

- `jansu-model` intentionally keeps Kafka protocol vocabulary because it is the
  workspace protocol-model surface, not a runtime streaming client.
- Runtime adapters remain in `jansu-broker`, `jansu-client`, `jansu-cat`,
  `jansu-cli`, `jansu-embedded`, and `jansu-proxy`.
- The machine-readable routing entry lives in `agent/boundaries.toml` and
  expires on `2026-12-31`.

## Next Action

If a real runtime client or queue adapter appears in `jansu-model`, move it to
the adapter boundary and delete this exception instead of extending it.
