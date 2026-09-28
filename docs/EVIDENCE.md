# Engineering Evidence Index

Maps every claimed capability to real, inspectable proof: the code that implements it,
the test that verifies it, and the evidence (real CLI output, or an honestly-labeled
mockup where a GUI screenshot isn't safely capturable) that demonstrates it running.

| Capability | Code | Test(s) | Evidence |
|---|---|---|---|
| PostgreSQL OLTP (11-table synthetic fintech schema) | `src/oltp/`, `src/synthetic/` | `tests/test_oltp.py`, `tests/test_synthetic_generation.py` | [`screenshots/postgresql/`](assets/screenshots/postgresql/) — real table list + row counts |
| CDC — Debezium logical replication → Kafka | `docker/docker-compose.yml` (connector config) | — (infrastructure) | [`screenshots/docker/`](assets/screenshots/docker/), [`screenshots/kafka/`](assets/screenshots/kafka/) — connector `RUNNING` |
| CDC Bronze (immutable, checkpointed, idempotent) | `src/cdc/consumer.py`, `src/cdc/checkpoint.py` | `tests/test_cdc_bronze_integration.py`, `tests/test_cdc_checkpoint.py`, `tests/test_cdc_consumer_unit.py` | [`screenshots/cdc/`](assets/screenshots/cdc/) — real checkpoint file, zero lag |
| CDC Silver (current-state upsert, tombstone handling) | `src/cdc/silver.py`, `src/cdc/silver_types.py` | `tests/test_cdc_silver_integration.py`, `tests/test_cdc_silver_state_machine.py`, `tests/test_cdc_silver_validation.py` | [`architecture/diagrams/15-cdc-event-lifecycle.md`](../architecture/diagrams/15-cdc-event-lifecycle.md) |
| API ingestion — OSM, Frankfurter, GLEIF (live, no auth) | `src/ingestion/merchant_osm.py`, `currency.py`, `gleif.py` | `tests/test_merchant_osm.py`, `tests/test_currency.py`(via `test_gleif.py`) | [`screenshots/api/`](assets/screenshots/api/) — real Bronze metadata/watermarks |
| API reference data (MCC, country, ISO 4217, BIN/IIN) | `src/ingestion/mcc.py`, `country.py`, `iso_currency.py`, `card_issuer.py` | `tests/test_mcc.py`, `tests/test_country.py`, `tests/test_iso_currency.py`, `tests/test_card_issuer.py` | `docs/06-data-source-catalog.md` |
| Incremental/watermark ingestion | `src/ingestion/watermark.py` | `tests/test_watermark.py` | [`architecture/diagrams/12-incremental-ingestion.md`](../architecture/diagrams/12-incremental-ingestion.md) |
| Silver validation, quarantine, rejection routing | `src/silver/*.py`, `src/silver/quarantine.py` | `tests/test_silver_*.py` (10 files) | [`screenshots/data-quality/`](assets/screenshots/data-quality/) |
| Gold — facts, dimensions, surrogate keys, unknown member | `src/gold/dimensions.py`, `src/gold/facts.py`, `src/gold/keys.py` | `tests/test_gold_dimensions.py`, `test_gold_facts.py`, `test_gold_keys.py` | [`screenshots/duckdb/`](assets/screenshots/duckdb/) — real 25-table schema |
| Gold reconciliation (control-total identity) | `src/gold/reconciliation.py` | `tests/test_gold_reconciliation.py` | £435,106.16 — see [`architecture/diagrams/18-data-quality-flow.md`](../architecture/diagrams/18-data-quality-flow.md) |
| PySpark independent track (Bronze/Silver/Gold/Consumption) | `src/spark/` | `tests/test_spark_bronze_to_silver.py`, `test_spark_silver_to_gold.py`, `test_spark_gold_to_consumption.py` | [`screenshots/spark/`](assets/screenshots/spark/) — real 1,010→875+135 |
| dbt (staging → intermediate → marts, tests) | `dbt/models/`, `dbt/tests/` | `tests/test_dbt_gold_transformation.py` | [`screenshots/dbt/`](assets/screenshots/dbt/) — real `dbt build`, 73/73 |
| Airflow — 3 DAGs, independent schedules | `dags/merchantmcc_cdc_pipeline.py`, `_api_pipeline.py`, `_client_delivery.py` | — (orchestration, verified by live execution) | [`screenshots/airflow/`](assets/screenshots/airflow/) — real DAG list + run states |
| Client Data Delivery (entitlement, validation, manifest) | `src/delivery/pipeline.py`, `src/delivery/config.py` | `tests/test_delivery_config.py`, `test_delivery_pipeline.py` (25/25) | [`screenshots/client-delivery/`](assets/screenshots/client-delivery/) — real `outbox/` + manifest |
| Power BI semantic model + report (PBIP/TMDL/PBIR) | `reports/MerchantMCC_S15C_Executive_Overview.*` | — (schema-validated, not unit-testable) | [`screenshots/powerbi/`](assets/screenshots/powerbi/) — mockup + honest status |
| Security boundary (`.env` never committed) | `.gitignore`, `.env.example` | — | [`architecture/diagrams/17-security-boundary.md`](../architecture/diagrams/17-security-boundary.md) |

**Full test suite**: 760 tests collected; most recently observed complete run: 758
passed, 2 transient timeout failures (traced to a host-suspend event, both individually
re-confirmed passing afterward), 3 skipped. See [`screenshots/testing/`](assets/screenshots/testing/).
