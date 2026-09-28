# Kafka / Debezium Evidence

Real screenshots from a local Kafka GUI client connected to the `MerchantMCC Kafka`
cluster (`localhost:29092`), captured 2026-09-28.

### Topic browser

![Kafka topic browser](01-kafka-topics.png)

Every `finpay.finpay.*` topic is a real table Debezium is streaming — `campaigns`,
`cardholders`, `card_tokens`, `clients`, `offers`, `programs`, `reconciliation`,
`reward_events`, `settlements`, `transaction_events`, `transactions` — plus Kafka
Connect's own internal `finpay_connect_configs/offsets/statuses` topics and the two live
`finpay-cdc-bronze-consumer` consumer instances shown under **Consumers**.

### Transactions topic — real CDC messages

![finpay.finpay.transactions messages](finpay.finpay.transactions.png)

50 real Debezium change-event messages on partition 0 of `finpay.finpay.transactions`,
with real offsets and timestamps — proof that row-level changes in PostgreSQL are
actually reaching Kafka, not just that the topic exists.

## CLI evidence (real, captured 2026-09-28)

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

## Reproducing this evidence

```bash
docker exec merchantmcc_kafka /kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
curl -s http://localhost:8083/connectors/finpay-postgres-cdc-connector/status
docker exec merchantmcc_kafka /kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group finpay-cdc-bronze-consumer
```

No credentials or tokens appear in either screenshot or the commands above — none of the
brokers used here require authentication in this local setup, and the message payloads
shown are synthetic fintech events, not real financial data.
