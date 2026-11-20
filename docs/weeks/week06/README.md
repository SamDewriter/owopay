# Week 6 · Finish dbt, then hand it to Airflow

**What happens:** one class in two halves, run the same way as Week 5. You run every command on your laptop, projected, and walk through the files behind each step. Nobody types code in class; learners watch, then rebuild and extend it afterwards (part C).
- **First hour: dbt, finished.** Week 5 ran out of time before dbt was done. This time each model and each macro is explained and then executed, one layer at a time, from an empty warehouse.
- **Second hour: Airflow.** The extract and `dbt build` that we ran by hand become one DAG, `owopay_daily`, which runs every morning at 06:00 Lagos.

**Starting point:** whatever Week 5 reached, the data is there:
- `owopay` holds the seeded Lagos days;
- `dw.raw` holds the extracted copy.

Step 1 wipes everything dbt built, so the class starts from the same point however far Week 5 got.

**Ends with:**
- the star schema and finance mart rebuilt in front of the room, with every macro executed on its own;
- Airflow running `owopay_daily`. The room has watched it:
  - fail on a day with no data;
  - succeed on retry once the data lands;
  - add customer history;
  - rerun a past day without changing a single total.

**Status:** part A is built and was rehearsed on the instructor's Mac on 13 November 2026, with Airbyte still running:
- the dbt part from an empty warehouse: 50 of 50 nodes;
- the Airflow part from a missing image to a green run;
- `make check-w6` checks 1–6 PASS, and check 7 (the lab) FAIL, as expected;
- `make lint` and `make test` pass.

What remains: commit and push the template, and the morning-of checklist (A7).

---

## How the week is split

| Who | When | What |
| --- | --- | --- |
| You | Before class (part A) | The Week 6 code is written; build the Airflow image once and prepare the laptop |
| You, with the room watching | In class (part B) | Rebuild dbt layer by layer; start Airflow; run the DAG through a failure, a retry and a rerun |
| Learners | After class (part C) | Run the same on their machine, then write the runbook and their own DAG |

## Defaults

| Decision | Default | Why |
| --- | --- | --- |
| Where it runs in class | Your Mac, projected, offline: the images, venv and dbt packages are cached | Same as Week 5 |
| dbt starting point | An empty warehouse (`reset_warehouse`), whatever Week 5 reached | Everyone sees every layer built; no "we did this bit last week" |
| How Airflow runs dbt | dbt baked into the Airflow image in its own venv; the repo is mounted read-only | One copy of the code; Airflow only schedules it. The usual first setup before reaching for Cosmos or a Kubernetes pod |
| Schedule | 06:00 Lagos, loading yesterday's Lagos day | The day has closed and late settlements have had six hours to land |
| Retries | `extract`: 2, a minute apart (production: longer). `dbt_build`: none | Late data is worth retrying; a failed dbt test needs a person |
| One run at a time | `max_active_runs=1` | Two runs would delete and reload the same days at once |
| Spark (the `compute` profile) | Moves to Week 7 or later | Finishing dbt fills the first hour, and one new tool per class is enough |

---

## Part A · Before class: written, proved, to push

All of this is in the repo already. The list tells you what changed, so you can review it, and it is what learners pull after class.

### A1. Macros worth explaining (dbt)

Until now there was one macro, `generate_schema_name`, which is dbt plumbing. Now there are three of our own, each with a reason to exist, and a test of our own:

| File | What | Where it runs |
| --- | --- | --- |
| `dbt/macros/date_key.sql` | `date_key(col)`: yyyymmdd integer | `dim_date` makes the key and `fct_transactions` looks it up, through one macro, so the two can't drift apart |
| `dbt/macros/kobo_to_naira.sql` | `kobo_to_naira(expr)`: `/ 100.0`, exact numeric | `mart_daily_txn_summary.amount_naira` |
| `dbt/macros/reset_warehouse.sql` | drops `staging`, `snapshots`, `marts` (never `raw`); dry run unless `{dry_run: false}` | on its own: `dbt run-operation` |
| `dbt/tests/generic/not_in_the_future.sql` | a generic test: a test is a macro that returns the failing rows | `stg_transactions.created_at` in `_staging.yml` |

`dbt build` is now 50 nodes; it was 48 before the new `created_at` tests.

### A2. The extract fails on an empty day

**Touch:** `ingestion/batch/extract_to_dw.py`.
- If `--day` itself has no transactions in `owopay`, the script now exits non-zero and rolls back the whole run.
- `--allow-empty` overrides it.
- Without the guard, a run before the data arrives is green, the dashboard shows zero, and nobody is told.
- The command line is otherwise unchanged (the Week 5 contract).

### A3. Airflow that can run dbt

**Touch:**
- **new `airflow/Dockerfile`:** the official image plus `/opt/owopay-venv`, which holds dbt-core 1.12.6, dbt-postgres 1.11.0 and psycopg 3. It is a separate venv because dbt's dependencies and Airflow's conflict inside one environment.
- **`compose/stack.yml`, for the Airflow services:**
  - the image is `owopay-airflow:3.3.2`; only `airflow-init` builds it, and the others reuse it with `pull_policy: never`. If every service built it, five builds of one tag would collide.
  - environment `PG_HOST=postgres`, `PG_PORT=5432` and the other `.env` names, with the values Postgres has inside the network;
  - `DBT_USE_COLORS=false`;
  - read-only mounts of `generator/`, `ingestion/`, `dbt/` and `.env.example` at `/opt/owopay`.

The first build took about 12 minutes on your Mac with Airbyte running. After that, `make up PROFILE=orchestration` takes about 1 min 40 s.

### A4. The DAG

**Touch:** **new** `airflow/dags/owopay_daily.py`, read on screen in step 7:

```
resolve_day  ->  extract  ->  dbt_build
```

- **`resolve_day`:** the `day` param if given; otherwise yesterday in Lagos, from the run's logical date.
- **`extract`:** `@task.bash`. It runs the extract with `--lookback 2`.
- **`dbt_build`:** `@task.bash(retries=0)`, writing `target/` to the container's `/tmp`.
- **The DAG itself:**
  - `schedule='0 6 * * *'`, starting 1 November 2026 in Lagos;
  - `catchup=False`, `max_active_runs=1`;
  - `retries=2` with `retry_delay` of 1 minute;
  - tag `week6`.

### A5. Make targets and checks

**Touch:** `Makefile`, plus **new** `scripts/checks/check_w6.py`.

```make
trigger: .env ## Rerun owopay_daily for one past Lagos day: make trigger DAY=2026-11-10
	$(COMPOSE) exec airflow-scheduler airflow dags unpause owopay_daily
	$(COMPOSE) exec airflow-scheduler airflow dags trigger owopay_daily --conf '{"day": "$(DAY)"}'

check-w6: setup ## Week 6 checkpoints, headless
	$(PY) scripts/checks/check_w6.py
```

`make check-w6` runs these checks:
1. **Macros:** `date_key` rendered into the compiled fact; every naira equals kobo / 100.0; every fact day is in `dim_date`.
2. **Fact reconciles:** raw equals the fact, in rows and in kobo.
3. **Airflow up:** `owopay_daily` loaded, with no import errors.
4. **`owopay_daily` ran:** the latest run succeeded.
5. **Newest day loaded:** the newest day in `owopay` is in `raw`, row for row.
6. **History:** at least one customer has two versions.
7. **Lab:** `docs/runbooks/owopay_daily.md` exists and mentions `extract`.

### A6. Proved

Rehearsed on your Mac on 13 November, with Airbyte running (the timings will be faster without it):

| Step | Result |
| --- | --- |
| `make reset-warehouse CONFIRM=yes` | 3 schemas dropped, 7 s |
| `build -s staging` | 21 of 21, 8 s |
| `snapshot` | 50,000 rows, 6 s |
| `run -s dim_date dim_customer dim_merchant dim_agent` | 4 of 4, 6 s |
| `build -s fct_transactions` | 11 of 11, 17 s |
| `build -s mart_daily_txn_summary` | 4 of 4, 10 s |
| `build` | 50 of 50, 21 s |
| `make up PROFILE=orchestration` | 1 min 40 s, every service healthy |
| Unpause → scheduled run for 2026-11-12 | `extract` failed twice ("no transactions for 2026-11-12 yet"). The day was seeded (21 s) and try 3 succeeded; `dbt_build` took 30 s; the snapshot inserted 200 versions |
| `make trigger DAY=2026-11-10` | days 08–10 "replaced 100,000", snapshot `INSERT 0 0`, totals unchanged |

**Still to do:** commit and push, so learners can `git pull upstream`.

### A7. Your laptop, the morning of class

Unlike Week 5, don't run `make clean`. Part 1 resets only the warehouse, and the source data stays.

```bash
# 1. Docker Desktop running, then free its memory: Airbyte held 3.5-4 GB of 7.6, and Airflow needs 3
docker stop airbyte-abctl-control-plane              # docker start it after class

# 2. The day the DAG will load is yesterday. Check it is NOT seeded yet, so the room sees the failure.
cd ~/altschool/week4/owopay-platform
make up PROFILE=core
make psql      # select max((created_at at time zone 'Africa/Lagos')::date) from transactions;
               # must be before yesterday. On 13 Nov it was 2026-11-12 (the rehearsal's day).

# 3. Airflow: clean history, DAG paused, then stop it, so step 6 shows it starting
make up PROFILE=orchestration
docker compose exec airflow-scheduler airflow dags delete owopay_daily --yes   # the DAG reappears paused, no runs
make down
```

If the data is gone (someone ran `make clean`), restore the Week 5 end state before class, in about 10 minutes:
1. `make up PROFILE=core`
2. `make seed-week && make seed DAY=2026-11-10 ROWS=100000`
3. `make extract-week && make extract DAY=2026-11-10`

The rehearsal also seeded 2026-11-12, so don't seed 11 or 12 by hand. Customer changes are permanent, and history only appears for a day the snapshot hasn't seen.

Projector checklist:
- the editor open on `dbt/macros/`;
- a second terminal tab for `make psql DB=dw`;
- a browser tab ready for http://localhost:8080.

---

## Part B · In class: run it, read it

The same rhythm as Week 5: the idea in two minutes, run, walk through the files, then the checkpoint. If a command fails, read the first red line aloud, then use the troubleshooting table.

| Clock | Step |
| --- | --- |
| 0:00 | Where Week 5 stopped; today's two halves |
| **Part 1 · dbt** | |
| 0:05 | 1 · Back to an empty warehouse |
| 0:15 | 2 · Macros: SQL that writes SQL |
| 0:30 | 3 · Staging and a test of our own |
| 0:40 | 4 · Snapshot and dimensions |
| 0:52 | 5 · The fact and the mart |
| **Part 2 · Airflow** | |
| 1:05 | 6 · Airflow up |
| 1:15 | 7 · The DAG |
| 1:25 | 8 · The morning run: a failure, late data, a retry |
| 1:40 | 9 · Rerun a past day |
| 1:47 | 10 · Check and lab brief |
| 1:55 | Exit ticket |

### Every command, in order

This is the whole class on one screen, in the order you run it: keep it open beside the terminal. The steps below explain each one. Lines starting with `UI:` are clicks in Airflow at http://localhost:8080, and `sql>` lines go into `make psql DB=dw`. Replace `<yesterday>` with the day before the class, e.g. `2026-11-13`.

```bash
# ── Part 1 · dbt ────────────────────────────────────────────────────────────────

# Step 1 · Back to an empty warehouse
make up PROFILE=core
make check-w5
make reset-warehouse
make reset-warehouse CONFIRM=yes
make check-w5

# Step 2 · Macros
make dbt ARGS="show --inline \"select {{ kobo_to_naira('12345') }} as naira\""
make dbt ARGS="show --inline \"select {{ date_key('current_date') }} as today_key\""
make dbt ARGS="compile --inline \"select {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }}\""
make dbt ARGS="compile -s fct_transactions"

# Step 3 · Staging
make dbt ARGS="build -s staging"
make dbt ARGS="show -s stg_customers --limit 3"

# Step 4 · Snapshot and dimensions
make dbt ARGS="snapshot"
make dbt ARGS="run -s dim_date dim_customer dim_merchant dim_agent"

# Step 5 · The fact and the mart
make dbt ARGS="show --inline \"select * from {{ ref('dim_customer') }}\" --limit 3"
make dbt ARGS="build -s fct_transactions"
make dbt ARGS="build -s mart_daily_txn_summary"
make dbt ARGS="build"
make check-w5
make dbt ARGS="docs generate"                                                    # optional: lineage
.venv/bin/dbt docs serve --project-dir dbt --profiles-dir dbt --port 8085        # optional; Ctrl-C to stop

# ── Part 2 · Airflow ────────────────────────────────────────────────────────────

# Step 6 · Airflow up
make up PROFILE=orchestration
make ps
#   UI: open http://localhost:8080; owopay_daily is listed, paused

# Step 7 · The DAG
#   UI: owopay_daily → Graph (no commands; walk through airflow/dags/owopay_daily.py)

# Step 8 · The morning run, the late data, the retry
#   UI: unpause owopay_daily → open the new run → extract → Logs (attempt 1 fails)
make seed DAY=<yesterday> ROWS=100000
#   UI: watch extract retry and turn green, then dbt_build (if retries ran out: extract → Clear)
make psql DB=dw
#   sql> select customer_id, version_number, kyc_tier, state, valid_from, valid_to, is_current
#        from marts.dim_customer
#        where customer_id in (select customer_id from marts.dim_customer group by 1 having count(*) > 1 limit 1)
#        order by version_number;
#   sql> \q

# Step 9 · Rerun a past day
make trigger DAY=2026-11-10
#   UI: the new manual run → extract → Logs ("replaced"), dbt_build → Logs ("INSERT 0 0")
make check-w5

# Step 10 · Check
make check-w6
```

After class, put the laptop back:

```bash
make down
docker start airbyte-abctl-control-plane
```

### Step 1 · Back to an empty warehouse (0:05, 10 min)

**Idea:** dbt owns `staging`, `snapshots` and `marts`, and can rebuild them all from `raw`. `raw` belongs to the extract, and dbt never touches it.

**Run:**

```bash
make up PROFILE=core
make check-w5                                            # where we are, whatever Week 5 reached
make reset-warehouse                                     # dry run: prints the three drops
make reset-warehouse CONFIRM=yes                         # really drops them
make check-w5                                            # 1 PASS (raw); 3-5 FAIL: nothing built
```

**Walk through:** `dbt/macros/reset_warehouse.sql`. Point at the line make prints first: `dbt run-operation reset_warehouse --args '{dry_run: true}'`. That is the real dbt command; the make target only saves typing the quotes.
- A macro can run on its own: `run-operation`.
- `run_query` executes SQL; `log` prints.
- The `dry_run` default exists because dropping a schema is the kind of command you run once by mistake.

**Checkpoint:** `check-w5` 1 PASS, 3–5 FAIL. Check 2 only reads the result of the last dbt command, which was the reset itself. Say it out loud: dropping `snapshots` destroyed customer history. On a laptop that's fine; in production it never happens.

### Step 2 · Macros: SQL that writes SQL (0:15, 15 min)

**Idea:**
- A model is a Jinja template. dbt renders it into plain SQL, then sends that SQL to Postgres.
- `{{ ref() }}`, `{{ source() }}` and `{{ config() }}` are macros too. A macro is a function that returns SQL text.

**Run:**

```bash
make dbt ARGS="show --inline \"select {{ kobo_to_naira('12345') }} as naira\""      # 123.45
make dbt ARGS="show --inline \"select {{ date_key('current_date') }} as today_key\""   # e.g. 20261114
make dbt ARGS="compile --inline \"select {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }}\""
make dbt ARGS="compile -s fct_transactions"
```

**Walk through:**
- `macros/kobo_to_naira.sql` and `macros/date_key.sql`.
  - `{% macro %}` … `{% endmacro %}`, the arguments, and the `-` that trims whitespace.
  - Why `date_key` exists: `dim_date` and the fact must produce the same key, so they share one definition.
- `macros/generate_schema_name.sql`: overriding one of dbt's own macros. This is why the schemas are named `marts` and not `analytics_marts`.
- `dbt_utils.generate_surrogate_key`: a package macro, and the `md5(…)` it expands into.
- **Side by side:**
  - `models/marts/core/fct_transactions.sql` (Jinja);
  - `dbt/target/compiled/owopay/models/marts/core/fct_transactions.sql` (what Postgres actually received: `ref`s become `"dw"."marts"."dim_customer"`, and `date_key` becomes `to_char(...)`).

**Checkpoint:** the room can point to where each `{{ }}` went in the compiled file.

### Step 3 · Staging and a test of our own (0:30, 10 min)

**Run:**

```bash
make dbt ARGS="build -s staging"              # 21 of 21: 4 views and their tests
make dbt ARGS="show -s stg_customers --limit 3"   # no name, phone or BVN
```

**Walk through:**
- `models/staging/stg_transactions.sql`: rename and type, and nothing else.
- `tests/generic/not_in_the_future.sql`. A test is a macro that returns the bad rows, and zero rows means pass. `_staging.yml` uses it exactly like `unique`.
- **Where the extra two nodes came from:** `created_at` now has `not_null` and `not_in_the_future`.

**Checkpoint:** 21 of 21.

### Step 4 · Snapshot and dimensions (0:40, 12 min)

**Run:**

```bash
make dbt ARGS="snapshot"                                              # 50,000 customers, all version 1
make dbt ARGS="run -s dim_date dim_customer dim_merchant dim_agent"   # models only: 4 of 4
```

The snapshot prints the harmless `DATETIME` vs `DATETIMETZ` warning; say so before someone asks.

Why `run` and not `build` here:
- `build -s <dims>` also selects every test touching the dims.
- That includes the `relationships` tests from the fact, which doesn't exist yet. They error.
- `run` builds models only. Those tests run in step 5, once both sides exist.

**Walk through:**
- `snapshots/snap_customers.yml`: the timestamp strategy.
- `models/marts/core/dim_customer.sql`: the version number, the back-dated first version, and the window.
- `dim_date.sql`: the `date_key` macro again.

**Checkpoint:** every customer has one version. Step 8 adds the second.

### Step 5 · The fact and the mart (0:52, 13 min)

**Run:**

```bash
make dbt ARGS="show --inline \"select * from {{ ref('dim_customer') }}\" --limit 3"
make dbt ARGS="build -s fct_transactions"           # 11 of 11: the model, relationships, reconciliation
make dbt ARGS="build -s mart_daily_txn_summary"     # 4 of 4
make dbt ARGS="build"                               # everything, in order: 50 of 50
make check-w5                                       # 1, 2, 3, 5 PASS; 4 waits for step 8
```

Preview a big model from its built table, with `show --inline "select * from {{ ref(...) }}"`. Never use `show -s fct_transactions`: it re-runs the model's whole join over every transaction, plus each test. In rehearsal it ran for 12 minutes and had to be killed.

**Walk through:**
- `fct_transactions.sql`: the grain, the left joins, the point-in-time join to the customer, and the post-hook.
- `tests/assert_fct_reconciles_to_raw.sql`.
- `mart_daily_txn_summary.sql`: `kobo_to_naira`, used at the last column only.
- The lineage, if time allows:
  1. `make dbt ARGS="docs generate"`
  2. `.venv/bin/dbt docs serve --project-dir dbt --profiles-dir dbt --port 8085`

**Checkpoint:** 50 of 50.

### Step 6 · Airflow up (1:05, 10 min)

**Idea:** so far a person ran each command in order. Every morning at 06:00 nobody is awake to do it, so Airflow runs them.

**Run:**

```bash
make up PROFILE=orchestration     # about 1 min 40 s; talk through the files while it starts
make ps
```

**Walk through:**
- `airflow/Dockerfile`: the official image plus a separate venv for dbt.
- `compose/stack.yml`, under `x-airflow-common`:
  - `PG_HOST=postgres`: inside the network, Postgres is a hostname, not `localhost:5433`;
  - the read-only mounts: Airflow runs the code we already have; it doesn't own a copy;
  - the four services and what each does (api-server, scheduler, dag-processor, triggerer).
- Open http://localhost:8080. There's no login (`SIMPLE_AUTH_MANAGER_ALL_ADMINS`, local only). `owopay_daily` is paused.

**Checkpoint:** every service healthy, and the DAG listed with no import errors.

### Step 7 · The DAG (1:15, 10 min)

**Walk through** `airflow/dags/owopay_daily.py` in the editor, then the Graph view in the UI:
- **No business logic in the DAG.** `extract` and `dbt_build` are the same commands we ran by hand.
- **`schedule`, `start_date`, `catchup=False`:** when it runs, and why it doesn't backfill every day since 1 November.
- **`resolve_day`:** yesterday in Lagos, or the `day` param.
- **`retries`:** two for `extract`, none for `dbt_build`, and why.
- **`max_active_runs=1`.**

### Step 8 · The morning run: a failure, late data, a retry (1:25, 15 min)

**Idea:** the most common failure is data that isn't there yet. A pipeline that goes green on an empty day is worse than one that goes red.

**Run:**
1. In the UI, unpause `owopay_daily`. Airflow immediately creates the run for this morning's 06:00, which loads yesterday.
2. Open the run and the `extract` log. It reads `owopay has no transactions for <yesterday> yet: nothing committed`, and the task turns orange (`up_for_retry`).
3. Now the data arrives late. Seed yesterday:

```bash
make seed DAY=<yesterday> ROWS=100000      # about 20 s; the next retry is at most a minute away
```

4. Watch the retry. On try 2 or 3, `extract` turns green, then `dbt_build` runs (about 30 s).

If the retries run out before the seed finishes, select `extract` and choose **Clear**: it runs again and the run continues. That is also a lesson: clearing a task is how you rerun one.

**Walk through** the logs in the UI:
- `extract`, attempt 1 next to attempt 3.
- `dbt_build`: the snapshot line, which reads `INSERT 0 200`.

Then show the history in `make psql DB=dw`, with the step 7 query from Week 5:

```sql
select customer_id, version_number, kyc_tier, state, valid_from, valid_to, is_current
from marts.dim_customer
where customer_id in (select customer_id from marts.dim_customer group by 1 having count(*) > 1 limit 1)
order by version_number;
```

**Checkpoint:** the run is green and about 200 customers have two versions. No person ran dbt.

### Step 9 · Rerun a past day (1:40, 7 min)

**Idea:** finance says Tuesday's numbers look wrong, so reload it. A rerun must replace data, never add to it.

**Run:**

```bash
make trigger DAY=2026-11-10
make check-w5          # same totals as before the rerun
```

**Walk through** the `extract` log, which shows days 08–10 "replaced 100,000", and the snapshot's `INSERT 0 0`. These are the delete-and-reload from Week 5 and the timestamp strategy, which is what makes a rerun safe.

### Step 10 · Check and lab brief (1:47, 8 min)

```bash
make check-w6          # 1-6 PASS; 7 waits for the runbook
```

Brief part C, starting with its first row.

---

## Part C · Lab (due Day 7, 23:59)

| Do | Touch | Prove with |
| --- | --- | --- |
| **First: rebuild what you watched.** Steps 1–2: `git pull upstream main`, then `make setup`. Then `make up PROFILE=core`, reset and `make dbt ARGS="build"` as in part 1. Step 3: `make up PROFILE=orchestration` (the first time builds the image: about 10 minutes, with internet). Step 4: unpause, then `make seed DAY=<your yesterday> ROWS=100000` | nothing | `make check-w6` 1–6 |
| A macro of your own, used in your Week 5 mart (for example `percent_of(part, whole)`) | **new** `dbt/macros/<name>.sql`, your mart | `make dbt ARGS="compile -s mart_<name>"`: paste the compiled line in the PR |
| Runbook for `owopay_daily`: for each task, what failure looks like, the first thing to check, and how to rerun safely | **new** `docs/runbooks/owopay_daily.md` | `make check-w6` check 7 |
| Week 4 bridge lab: your own DAG and producer | **new** `airflow/dags/<yours>.py` | `make check-w4` stage 3 |
| Week 5 lab, if not finished: ADR-002, data-model doc, your mart, the regulator mart | as in Week 5 | `make check-w5` |
| Stretch: a `freshness` task running `dbt source freshness` between `extract` and `dbt_build` | `owopay_daily.py` | the Graph view shows four tasks; the run is green |

Learners' numbers match each other's for the same day, but their "yesterday" differs from the class's, so they won't match the class.

**Accepted when:** `make check-w6` is green on their machine, and the PR has a screenshot of the green run and of the failed-then-retried `extract`.

---

## What Week 7 relies on (do not rename)

- **DAG:** `owopay_daily`, with tasks `resolve_day`, `extract`, `dbt_build`.
- **Param:** `day` (YYYY-MM-DD).
- **Image:** `owopay-airflow:3.3.2`, with `/opt/owopay-venv` and the repo mounted at `/opt/owopay`.
- **Extract:** fails on an empty day unless `--allow-empty`.

## Files at a glance

```
Written before class (part A)
  dbt/macros/date_key.sql, kobo_to_naira.sql, reset_warehouse.sql      new
  dbt/tests/generic/not_in_the_future.sql                              new
  dbt/models/marts/core/dim_date.sql, fct_transactions.sql             changed: date_key macro
  dbt/models/marts/finance/mart_daily_txn_summary.sql                  changed: kobo_to_naira
  dbt/models/staging/_staging.yml                                      changed: created_at tests
  ingestion/batch/extract_to_dw.py                                     changed: fails on an empty day
  airflow/Dockerfile, airflow/dags/owopay_daily.py                     new
  compose/stack.yml, Makefile                                          changed: build, env, mounts; reset-warehouse, trigger, check-w6
  scripts/checks/check_w6.py                                           new

Walked through in class (part B), in this order
  dbt/macros/reset_warehouse.sql                                        step 1
  dbt/macros/*, target/compiled/…/fct_transactions.sql                  step 2
  dbt/models/staging/*, dbt/tests/generic/not_in_the_future.sql         step 3
  dbt/snapshots/snap_customers.yml, dim_customer.sql, dim_date.sql      step 4
  fct_transactions.sql, assert_fct_reconciles_to_raw.sql, mart_*.sql    step 5
  airflow/Dockerfile, compose/stack.yml (x-airflow-common)              step 6
  airflow/dags/owopay_daily.py                                          step 7

Written by learners (part C)
  dbt/macros/<theirs>.sql, docs/runbooks/owopay_daily.md, airflow/dags/<theirs>.py
```

## When something goes wrong

| Symptom | Cause | Fix |
| --- | --- | --- |
| `build -s dim_…` errors on `relationships_fct_transactions_…` | selecting a model also selects tests from models not built yet | `run -s` for the dims; the tests run with the fact (step 4) |
| `show -s fct_transactions` never returns | it re-runs the whole join and every test | Ctrl-C; use `show --inline "select * from {{ ref('fct_transactions') }}"` |
| `make: unrecognized option '--args'` | `make dbt ARGS=…` typed without its quotes, so the shell split it | use `make reset-warehouse [CONFIRM=yes]`; for any other `make dbt`, copy the whole line with its quotes |
| `make up PROFILE=orchestration` is slow the first time | it is building `owopay-airflow` (pip installs dbt) | wait; only the first time. Without internet it can't build: build before class |
| Airflow services restart or get killed | out of memory: core + Airflow need about 6 GB of Docker's memory | stop other containers (Airbyte); Docker Desktop → Resources → 8 GB |
| `extract` fails: `owopay has no transactions for … yet` | the day hasn't been seeded: the guard working | `make seed DAY=…`, then let the retry run or **Clear** the task |
| A manual run sits `queued` | another run of the DAG is active (`max_active_runs=1`) | wait for it, or mark the other one failed |
| `dbt_build` fails in Airflow but `make dbt` works | `.env` values that only make sees | the container gets its settings from `compose/stack.yml`; compare `PG_*` there |
| No second customer version after the run | that day's customer changes were already seeded before the snapshot first ran | seed a day the snapshot hasn't seen (A7) |
| dbt warns `DATETIME` vs `DATETIMETZ` on the snapshot | dbt's type check, not the data | ignore it |
