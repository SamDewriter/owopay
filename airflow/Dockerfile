# The course's Airflow image: the official one plus a separate venv holding dbt and psycopg 3.
# A separate venv, because dbt's dependencies and Airflow's would fight inside one environment.
# Built once by `make up PROFILE=orchestration` (it needs the internet that first time only).
ARG AIRFLOW_IMAGE
FROM ${AIRFLOW_IMAGE}

USER root
RUN mkdir -p /opt/owopay-venv && chown airflow: /opt/owopay-venv
USER airflow

# Same versions as the laptop venv (pyproject.toml), so dbt behaves the same in both places.
RUN python -m venv /opt/owopay-venv \
 && /opt/owopay-venv/bin/pip install --no-cache-dir \
      "dbt-core==1.12.6" "dbt-postgres==1.11.0" "psycopg[binary]>=3.2,<4"
