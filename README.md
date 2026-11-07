# OwoPay data platform

The repository the data engineering programme builds on from Week 4 to Week 12. OwoPay is a fictional Nigerian payments company: a million transactions a day on one Postgres, a finance dashboard that times out, a regulator who wants T+1 to the kobo, and a fraud team that wants minutes. Every week ends in a merged pull request on this repo; nothing is thrown away.

The stack and every choice behind it are in [ADR-000](docs/adr/ADR-000-course-stack.md). The source data is described in [docs/schema.md](docs/schema.md). Each week's plan is under [docs/weeks/](docs/weeks/) (start with [Week 5](docs/weeks/week05/README.md)).

## Start here

You need Docker with Compose v2 and Python 3.11–3.13, or a GitHub Codespace (open this repo in one and skip to step 2).

```bash
make doctor                               # can this machine run it?
make up PROFILE=core                      # Postgres, Redpanda, SeaweedFS (2.75 GB)
make seed DAY=2026-11-03 ROWS=200000      # one Lagos day of OwoPay, ~30 seconds
make demo                                 # what is in the three systems now
```

From Week 5, the warehouse:

```bash
make seed-week                            # the course week: 7 Lagos days, 800,000 transactions
make extract-week                         # copy them into dw.raw
make dbt ARGS="deps" && make dbt ARGS="build"   # staging, snapshot, star schema, marts, tests
make check-w5
```

`make help` lists every target. The first `make` that needs Python creates `.venv` and installs the project; the first one that needs settings copies `.env.example` to `.env`.

| Where | URL |
| --- | --- |
| Redpanda Console | http://localhost:8082 |
| SeaweedFS file browser | http://localhost:8888 |
| Airflow (`make up PROFILE=orchestration`) | http://localhost:8080 |
| Postgres | `make psql` or `make psql DB=dw` |
| dbt docs (`make dbt ARGS="docs generate"`, then `.venv/bin/dbt docs serve --project-dir dbt --profiles-dir dbt --port 8085`) | http://localhost:8085 |

## Profiles

One profile at a time keeps an 8 GB laptop comfortable; `core` always runs.

| Profile | Runs | Memory | From |
| --- | --- | --- | --- |
| `core` | Postgres, Redpanda + Console, SeaweedFS | 2.75 GB | Week 4 |
| `orchestration` | Airflow 3 (api-server, scheduler, dag-processor, triggerer) | 3 GB | Week 4 |
| `compute` | Spark master and worker (Jupyter joins in Week 6, +0.5 GB) | 2 GB | Week 6 |
| `serving` | Metabase | 1 GB | Week 9 |
| `observe` | Prometheus, Grafana, statsd-exporter | 0.5 GB | Week 12 |
| `all` | everything | ~9.25 GB | Weeks 9–12 |

## When something is wrong

- **A port is taken** (most often a local Postgres on 5432): `make doctor` names the program. Stop it, or move our port in `.env`, for example `PG_PORT=5433`. Everything (compose, scripts, notebooks) reads `.env`.
- **Containers restart or hang:** give Docker more memory (6 GB on an 8 GB laptop) or run fewer profiles.
- **Windows:** use WSL2 and clone the repo inside the WSL filesystem (`~/`), not under `/mnt/c`.
- **Start over:** `make clean` deletes every database, topic and bucket; `make up` and `make seed` rebuild them.

## Layout

| Path | What lives there | Week |
| --- | --- | --- |
| `docker-compose.yml`, `compose/stack.yml` | the services; `versions.env` pins every image | 4 |
| `generator/owopay/` | the OwoPay generator behind `make seed` | 4 |
| `scripts/` | `demo`, `doctor` and the weekly `check-wN` | 4 |
| `notebooks/` | one notebook per week | 4 |
| `ingestion/bridge/` | your Kafka producer from earlier weeks | 4 |
| `airflow/dags/` | DAGs; `owopay_smoke` proves Airflow reaches the stack | 4 |
| `docs/adr/`, `docs/design/` | decision records and the system design doc | 4 |
| `ingestion/batch/` | the extract from `owopay` into `dw.raw` | 5 |
| `dbt/` | the warehouse model in `dw` | 5 |
| `spark/` | the large transform and Delta writes | 6 |
| `quality/` | GX Core suites and contracts | 8 |
| `observe/` | Prometheus and Grafana provisioning | 12 |
| `.github/workflows/` | CI, switched on in Week 8 | 8 |

## Rules of the repo

- Secrets live only in `.env`, which is git-ignored.
- One way to do each job: alternatives are recorded in an ADR, not installed.
- Every number has its assumption beside it; every decision has its ADR.
