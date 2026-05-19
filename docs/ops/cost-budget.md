# Cost Budget

This budget applies to CI, local integration services, object storage used by
tests, and differential Kafka proof.

## Monthly Caps

| Resource | Soft cap | Hard cap |
| --- | ---: | ---: |
| GitHub Actions minutes | 15000 min | 20000 min |
| Differential Kafka lab runs | 100 runs | 150 runs |
| S3-compatible test storage | 200 USD | 400 USD |
| Container registry storage | 100 USD | 250 USD |

## Budgets, Quotas, Kill Switches

Each cost surface above has an explicit monthly quota (the soft cap) and a kill
switch (the hard cap).  The budget is the hard cap; the stop condition is the
soft cap.  When the soft cap is crossed, log an audit note and pause new paid
work; when the hard cap is crossed, the kill switch is mandatory — stop the
release/migration lane, tear down the affected service, and route a follow-up
audit item under `cross-phase`.

## Stop Conditions

- Stop and record an audit note when a soft cap is crossed (the per-month
  quota).
- Stop release or migration work when a hard cap is crossed (the kill switch
  trip point).
- Tear down local services with `docker compose down --remove-orphans --volumes`
  when a test stack is no longer needed.
- Do not run broad matrix tests to investigate a narrow failure until the
  focused lane has been run and recorded.
- Before starting Docker-backed or differential proof work, note the exact
  command, expected artifact path, and kill switch in the attempt log so the
  next agent can resume or stop without re-deriving the budget.
