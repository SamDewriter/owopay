# ADR-000: The course stack

- Status: accepted by class vote, Week 4 (record the date and the vote here)
- Deciders: the cohort and the instructor
- Full record: the Course Tech Stack document (criteria, per-component specification, alternatives considered)

## Context

Nine weeks remain. An 8 GB laptop and no credit card are the floor. Airflow and the Kafka API are prerequisites. MinIO, the usual classroom object store, was archived in April 2026. Learners work in GitHub Codespaces first and on their own machines from Week 5.

## Options considered

Per layer, in the alternatives table of the stack document. Each option that lost names the criterion it fails: free to run, 8 GB laptop, builds on what we know, hiring signal, one way to do each job, local-first and cloud-aware.

## Decision

Postgres 17 (source and warehouse), Redpanda (Kafka API), SeaweedFS (S3), Delta Lake, dbt Core, Spark, DuckDB, Airflow 3, dbt tests + GX Core + JSON Schema, Metabase, Prometheus + Grafana. They run as Docker Compose profiles with every image pinned in `versions.env`, Codespaces is the first environment and the standing fallback, and GitHub Actions is the CI.

## Consequences

- Every learner runs the same commands and the stack costs nothing to run.
- The single-node stack differs from production in resilience; Week 12 makes the gap explicit and prices the managed equivalents.
- SeaweedFS is new to the cohort. RustFS is the drop-in swap if it causes friction; the Week 5 class is the deadline for that decision.
- Pins are reviewed before Weeks 6, 8 and 10.

## Amendments

- Week 4 build: images are pinned to exact tags rather than digests (`chrislusf/seaweedfs:4.52`, not `latest`). Redpanda runs with `--memory=768M` inside its 1 GB limit so the container is not killed. Postgres's host port is configurable (`PG_PORT`) because learners with a local Postgres hit 5432. DuckDB stays on 1.x although 2.0 is out.
