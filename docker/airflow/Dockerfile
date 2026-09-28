# MerchantMCC-DE's own Airflow image — independent of any other project's
# Airflow deployment on this machine (see docs/30 or the Airflow
# Orchestration Audit report for the finding that motivated this).
#
# Base image: apache/airflow:3.3.1-python3.12 — a current stable Apache
# Airflow 3.x release, matching this project's own `requires-python
# = ">=3.12"` (pyproject.toml). 3.3.1 was chosen deliberately, not copied
# from another project's compose file: it is a real, current stable 3.x
# release, and Python 3.12 keeps this image's interpreter version
# consistent with the rest of this project's tooling (the Windows venv,
# `.venv-spark`, and pyproject.toml all target 3.12+).
FROM apache/airflow:3.3.1-python3.12

# Only application-level libraries this project's own pipeline modules
# need (see requirements.txt) — no extra Airflow providers, no Celery/
# Redis, nothing beyond what src/cdc/consumer.py, src/cdc/silver.py,
# src/gold/pipeline.py, and dbt/ actually import.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
