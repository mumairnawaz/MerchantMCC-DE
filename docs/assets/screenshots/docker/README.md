# Docker Evidence

Real screenshot from Docker Desktop, captured 2026-09-28.

### Container list

![Docker Desktop container list](01-docker-containers.png)

The real MerchantMCC container topology: `merchantmcc_connect` (Debezium),
`merchantmcc_postgres` (application OLTP), `merchantmcc_kafka`,
`merchantmcc_airflow_postgres` (Airflow's own metadata database — a separate instance
from the application database), and 3 Airflow 3 components
(`merchantmcc_airflow_apiserver`, `merchantmcc_airflow_scheduler`,
`merchantmcc_airflow_dag_processor`), all healthy. Two unrelated containers from other
local projects on this machine (`fball-elites`, a bare `docker` entry) are visible in the
same view and are not part of MerchantMCC-DE.

## CLI evidence (real, captured 2026-09-28)

```
NAME                                IMAGE                                    STATUS
merchantmcc_airflow_apiserver       merchantmcc-airflow:3.3.1-python3.12    Up 24 hours
merchantmcc_airflow_scheduler       merchantmcc-airflow:3.3.1-python3.12    Up 24 hours
merchantmcc_airflow_dag_processor   merchantmcc-airflow:3.3.1-python3.12    Up 24 hours
merchantmcc_airflow_postgres        postgres:16-alpine                       Up 3 days (healthy)
merchantmcc_kafka                   quay.io/debezium/kafka:3.0.0.Final        Up 4 days (healthy)
merchantmcc_postgres                postgres:16-alpine                       Up 4 days (healthy)
merchantmcc_connect                 quay.io/debezium/connect:3.0.0.Final      Up 4 days (healthy)
```

No environment variables or secrets are exposed in the screenshot or `docker ps` output —
both only show container name, image, and status.
