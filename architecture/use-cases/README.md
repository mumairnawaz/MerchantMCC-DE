# Use Cases

Who interacts with MerchantMCC-DE, and through which real, implemented capability — not
hypothetical functionality. Mermaid doesn't have native UML use-case notation, so each
diagram below is a `flowchart` connecting an actor to the specific capabilities they
actually use.

## 1. Data Engineer

```mermaid
flowchart LR
    DE(["Data Engineer"])
    DE --> C1["Run/extend API ingestion<br/>scripts/run_ingestion.py, src/ingestion/"]
    DE --> C2["Run/extend CDC pipeline<br/>src/cdc/ — consumer, silver, checkpoint"]
    DE --> C3["Rebuild Gold<br/>src/gold/pipeline.py"]
    DE --> C4["Author/modify dbt models<br/>dbt/models/"]
    DE --> C5["Author/modify Airflow DAGs<br/>dags/"]
    DE --> C6["Extend the Spark track<br/>src/spark/"]
    DE --> C7["Write/run tests<br/>tests/ — 760 collected"]
    DE --> C8["Extend Client Delivery datasets<br/>configs/delivery_datasets.json"]
```

## 2. BI Analyst

```mermaid
flowchart LR
    BI(["BI Analyst"])
    BI --> C1["Explore marts via Power BI semantic model<br/>reports/MerchantMCC_S15C_Executive_Overview.pbip"]
    BI --> C2["Build/modify measures and report pages<br/>TMDL semantic model, PBIR report definition"]
    BI --> C3["Query marts directly via DuckDB ODBC<br/>marts.transaction_mart, marts.merchant_mart, ..."]
```

## 3. Internal Business Stakeholder

```mermaid
flowchart LR
    STK(["Internal Business Stakeholder"])
    STK --> C1["View Power BI reports<br/>Executive Overview, Transaction & Merchant Analytics"]
    STK --> C2["Trust reconciled totals<br/>Approved = Settlement = Reconciliation Expected = £435,106.16"]
    STK --> C3["Understand merchant/MCC/reward performance<br/>merchant_mart, rewards_mart"]
```

## 4. Issuer / Network / Program Owner (external client)

```mermaid
flowchart LR
    EXT(["Issuer / Network / Program Owner"])
    EXT --> C1["Receive a governed extract<br/>outbox/&lt;client_id&gt;/&lt;dataset&gt;/&lt;run_id&gt;/"]
    EXT --> C2["Validate against the manifest<br/>schema_version, record_count, control_total"]
    EXT --> C3["Load CSV or Parquet into their own system<br/>no dashboard access required"]
    EXT -.->|"only within their own entitlement"| C4["client_program_mart — row-filtered to their own client_id"]
```

## 5. Platform / Operations

```mermaid
flowchart LR
    OPS(["Platform / Operations"])
    OPS --> C1["Monitor 3 Airflow DAGs<br/>merchantmcc_cdc_pipeline, _api_pipeline, _client_delivery"]
    OPS --> C2["Check Kafka/Debezium health<br/>connector status, consumer lag"]
    OPS --> C3["Verify dbt build/test results<br/>73/73 passing, incl. control-total identity"]
    OPS --> C4["Review the validation gate before any client delivery<br/>src/delivery/pipeline.py"]
    OPS --> C5["Rotate/manage local secrets<br/>.env, never committed"]
```

Every capability referenced above maps to real, implemented code — see
[`../../docs/EVIDENCE.md`](../../docs/EVIDENCE.md) for the full capability → code →
test → evidence mapping.
