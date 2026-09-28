# 16. Deployment / Local Infrastructure

Everything runs locally via Docker Compose — no cloud infrastructure. Real container
names, confirmed running:

```mermaid
flowchart TB
    subgraph DOCKER["Docker (docker-compose.yml), network: merchantmcc_net"]
        subgraph APP["Application data"]
            PG["merchantmcc_postgres<br/>postgres:16-alpine"]
        end
        subgraph CDC_INFRA["CDC infrastructure"]
            KAFKA["merchantmcc_kafka<br/>quay.io/debezium/kafka:3.0.0.Final"]
            CONNECT["merchantmcc_connect<br/>quay.io/debezium/connect:3.0.0.Final"]
        end
        subgraph AIRFLOW_INFRA["Airflow (own image: merchantmcc-airflow:3.3.1-python3.12)"]
            AF_PG["merchantmcc_airflow_postgres<br/>postgres:16-alpine — metadata DB only, never the app DB"]
            AF_SCHED["merchantmcc_airflow_scheduler"]
            AF_DAGP["merchantmcc_airflow_dag_processor"]
            AF_API["merchantmcc_airflow_apiserver<br/>:8081 → UI"]
        end
    end
    subgraph HOST["Host filesystem (bind-mounted)"]
        REPO["Project root<br/>mounted into every Airflow container"]
        DUCKDB[("data/gold/gold.duckdb")]
    end
    subgraph PYTHON["Local Python environments"]
        VENV[".venv — Windows, core pipeline + dbt + tests"]
        VSPARK[".venv-spark — WSL2, PySpark track"]
    end

    PG -->|logical replication| CONNECT
    CONNECT -->|produces to| KAFKA
    AF_SCHED -.->|reads/writes| DUCKDB
    AF_SCHED -.->|bind mount| REPO
    VENV -.->|runs against| DUCKDB
```

Seven containers, one Docker network (`merchantmcc_net`), two distinct PostgreSQL
instances (application data vs. Airflow's own metadata store — deliberately never the
same instance). No managed cloud service, no Kubernetes — a single-broker Kafka and a
single Debezium connector, appropriate for a local portfolio demonstration, not a
production-scale deployment claim.

`data/gold/gold.duckdb` is a single file on the host filesystem, bind-mounted into the
Airflow containers and opened directly by the local Python environment and (via ODBC) by
Power BI Desktop — there is no separate database server process for the warehouse layer.
