"""FinPay CDC layer (Phase S11): PostgreSQL logical replication -> Debezium ->
Apache Kafka.

Scope boundary (docs/24-postgresql-debezium-kafka-cdc.md): this package
configures and verifies PostgreSQL's publication/replication-slot state and
the Debezium Kafka Connect connector. It does NOT consume from Kafka into
Bronze — that is a later, explicitly separate phase. No Kafka client library
is used here; Kafka Connect's REST API is plain HTTP (via `requests`, already
an approved dependency), and Kafka broker/topic verification is done via
`docker exec` into the Kafka container using its own bundled CLI tools, not a
Python Kafka client (per this phase's explicit instruction).
"""
