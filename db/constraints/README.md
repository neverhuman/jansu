# Constraints

Bootstrap constraints are declared in `etc/initdb.d/010-schema.sql`.

When adding a constraint, update the owning storage tests and the compatibility
ledger if the constraint affects a Kafka-visible API.
