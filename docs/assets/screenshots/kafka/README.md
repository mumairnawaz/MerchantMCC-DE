# Kafka / Debezium Evidence

No GUI tool (e.g. a Kafka UI) is running in this environment, so the evidence below is
real CLI output captured directly against the live broker and connector — proving the
same facts a UI screenshot would, without needing one.

## CLI evidence (real, captured 2026-09-28)

**Real Kafka topics — one per source table, created by Debezium:**
```
finpay.finpay.campaigns
finpay.finpay.card_tokens
finpay.finpay.cardholders
finpay.finpay.clients
finpay.finpay.offers
finpay.finpay.programs
finpay.finpay.reconciliation
finpay.finpay.reward_events
finpay.finpay.settlements
finpay.finpay.transaction_events
finpay.finpay.transactions
```

**Debezium connector status — RUNNING:**
```json
{"name":"finpay-postgres-cdc-connector","connector":{"state":"RUNNING","worker_id":"172.21.0.4:8083"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"172.21.0.4:8083"}]}
```

**Consumer group lag — zero, across every topic:**
```
GROUP                      TOPIC                              LAG
finpay-cdc-bronze-consumer finpay.finpay.transaction_events   0
finpay-cdc-bronze-consumer finpay.finpay.settlements          0
finpay-cdc-bronze-consumer finpay.finpay.transactions         0
finpay-cdc-bronze-consumer finpay.finpay.reconciliation       0
finpay-cdc-bronze-consumer finpay.finpay.reward_events        0
finpay-cdc-bronze-consumer finpay.finpay.card_tokens          0
finpay-cdc-bronze-consumer finpay.finpay.cardholders          0
```
(offsets omitted here for brevity — full output available by re-running the commands
below; no credentials are involved in any of this output)

**Caption**: *"Kafka — Debezium-generated fintech CDC topics, connector RUNNING, zero
consumer lag."*

## Reproducing this evidence

```bash
docker exec merchantmcc_kafka /kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
curl -s http://localhost:8083/connectors/finpay-postgres-cdc-connector/status
docker exec merchantmcc_kafka /kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group finpay-cdc-bronze-consumer
```

## Manual screenshot checklist (optional, if a Kafka UI is added later)

- [ ] Topic list (equivalent to the CLI output above)
- [ ] Debezium connector status page
- [ ] A sample CDC message payload (redact nothing — these are synthetic fintech events,
      not real financial data)

No credentials or tokens appear in any of the commands above — none of the three brokers
used here require authentication in this local setup.
