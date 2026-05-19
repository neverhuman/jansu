# Authorization Matrix

Phase 14 owns full ACL and quota compatibility proof. Until that phase closes,
this matrix is a routing document and not a compatibility claim.

## Principals

- Anonymous listener principal: only valid on listeners configured without
  authentication.
- SASL user principal: established by the authentication handshake in
  `jansu-auth`.
- Broker principal: reserved for internal broker operations.

## Resources

- Topic: produce, fetch, list offsets, config, and deletion surfaces.
- Group: consumer group membership and committed offsets.
- Cluster: broker-wide metadata and controller-style operations.
- Transactional ID: transaction producer state.
- User: SCRAM credential management.

## Required Negative Proof

Before Phase 14 can advertise secured compatibility, each resource and
operation pair needs owner and non-owner tests. The owning tests should live in
the broker or service crate that routes the request and should prove both allow
and deny behavior through the same request path used by clients.

Current request-path negative proof lives in:

- `jansu-broker/tests/auth.rs::not_authenticated` for anonymous denial on the
  broker request path.
- `jansu-broker/tests/auth.rs::acl_requests_are_denied_on_the_broker_path` for
  ACL admin traffic reaching the broker's deny-by-default route.
- `jansu-broker/tests/compatibility_contract.rs::broker_safe_error_routes_return_structured_errors`
  for ACL and admin-style request routing through the broker's safe-error lane.

## Audit Routing

Until Phase 14 closes, broker authorization findings are tracked as proof gaps,
not compatibility claims. Fixes must add request-path tests or explicit attempt
log evidence for the affected resource before changing advertised support.
