# dbt: the OwoPay warehouse model

Built in Week 5. It turns the copies in `dw.raw` into a tested star schema and consumer marts in the `dw` database. Always run it through make from the repo root, so it reads `.env`:

```bash
make dbt ARGS="deps"                      # once: dbt_utils, dbt_expectations
make dbt ARGS="build"                     # snapshot, models and tests, in dependency order
make dbt ARGS="build -s fct_transactions+"   # one model and everything downstream
make dbt ARGS="docs generate"             # lineage; view with dbt docs serve --port 8085
```

| Folder | Schema in `dw` | What lives there |
| --- | --- | --- |
| `models/staging/` | `staging` | one view per raw table: renamed, typed, no personal data |
| `snapshots/` | `snapshots` | `snap_customers`: every version of every customer |
| `models/marts/core/` | `marts` | the star: `dim_date`, `dim_customer` (Type 2), `dim_merchant`, `dim_agent`, `fct_transactions` |
| `models/marts/finance/` | `marts` | `mart_daily_txn_summary`, finance's daily view |
| `tests/` | — | `assert_fct_reconciles_to_raw`: every Lagos day reconciles to the kobo |

The grain of `fct_transactions` is one row per transaction, in its latest status. Week 6 runs `dbt build` from Airflow and rebuilds the fact with Spark; model names here are a contract, so don't rename them.
