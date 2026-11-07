# Week 5 · Building the platform together, in class

**What happens:** one class where you bring OwoPay's platform to life on your own laptop, projected for the room. You run each command and, while it runs, open the code behind it and walk the room through it. Nobody types code in class, and learners don't run anything during it: they watch, ask, and then run the same commands on their own machines after class (part C).

**Starting point:** Week 4 decided the system in the room (the four questions, ADR-000, the lake-plus-warehouse argument) but nothing was built: no stack, no data, no repo. Week 5 starts from an empty machine.

**Ends with:** the room has watched the whole platform come up from nothing on your laptop:
- the core stack running;
- 800,000 transactions over 7 Lagos days in `owopay`;
- the same data copied into `dw.raw`;
- a tested star schema and a finance mart in `dw`, built by dbt.

Every learner leaves knowing what each command does and why each file is shaped the way it is. They have the commands to rebuild it themselves and a lab that extends it.

Week 6 puts the extract and `dbt build` under one Airflow DAG and scales the fact with Spark.

**Status:** part A is built and was rehearsed on the instructor's Mac from an empty stack on 7 November 2026. Every command in part B ran, `dbt build` passed 48 of 48 nodes, and `make check-w5` passed checks 1–5. What remains: commit and push the template (learners need it after class), and the morning-of checklist (A7).

---

## How the week is split

| Who | When | What |
| --- | --- | --- |
| You | Before class (part A) | Write all of Week 5's code, prove it from nothing, push it to the template, prepare your laptop |
| You, with the room watching | In class (part B) | Run every command on your laptop; walk through the files behind each step |
| Learners | After class (part C) | Run the same commands on their own machine, then write their own code: a mart for their question, the regulator mart, ADR-002, the data-model doc |

## Defaults (change them before part A)

| Decision | Default | Why |
| --- | --- | --- |
| Where it runs in class | Your Mac, projected. Everything it needs is cached, so the class needs no internet | One screen the whole room follows; no Wi-Fi, Codespace or laptop surprises in front of the class |
| Where learners run it | After class, on their own laptop or in a Codespace, from the template | They follow part C's first row: the same commands they watched |
| Class length | 2 hours | Same slot as Week 4; running and reading leaves room for questions |
| Seed size | 7 Lagos days: 2026-11-03 at 200,000 rows, 04–09 at 100,000 (800,000 transactions) | Enough history for snapshots and a visibly slow OLTP query. 2–3 minutes expected with Airbyte stopped; 7 min 45 s in rehearsal with it running |
| Fact grain | One row per transaction, latest status | Simplest correct counts. ADR-002 (lab) argues the alternatives |
| Customer history | Type 2 via a dbt snapshot | The generator changes 1 in 250 customers per day (KYC tier or state) |
| Transactions with no agent | Empty `agent_key` | An "unknown agent" row is the lab stretch |

---

## Part A · Before class: write it, prove it, push it

Work in the template repo on a branch: `git switch -c week-05`.

### A1. dbt in the project

**Touch:**
- `pyproject.toml`: add `"dbt-core==1.12.*"` and `"dbt-postgres>=1.9,<2"` to `dependencies`.
- `Makefile`:
  - load `.env` for every target, so dbt sees `PG_PORT` (`include .env` and `export` at the top);
  - in the `$(PY)` rule, set `SSL_CERT_FILE` from `certifi` before installing;
  - add the `dbt` target.

```make
dbt: setup ## Run dbt: make dbt ARGS="build -s staging"
	.venv/bin/dbt $(ARGS) --project-dir dbt --profiles-dir dbt
```

Why `SSL_CERT_FILE`:
- dbt-core 1.12 depends on `dbt-core-experimental-parser`, whose install downloads a wheel from GitHub.
- On python.org Python for macOS that download fails with `CERTIFICATE_VERIFY_FAILED`, and pointing `SSL_CERT_FILE` at `certifi`'s bundle fixes it.
- Checked 6 November 2026: dbt-core 1.12.6 and dbt-postgres 1.11.0 install that way.
- Codespaces is not affected, but Mac laptops (yours and learners') are.

**Done when:** `rm -rf .venv && make setup && .venv/bin/dbt --version` prints Core 1.12.x and postgres 1.11.x, and `make test` passes.

### A2. Make targets for every class command

Every command in part B has to be one short `make` line. **Touch:** `Makefile`.

```make
seed-week: setup ## Seed the course week: 2026-11-03 (200k), then 04-09 (100k each)
	$(PY) -m owopay.generate --day 2026-11-03 --rows 200000
	for d in 04 05 06 07 08 09; do $(PY) -m owopay.generate --day 2026-11-$$d --rows 100000 || exit 1; done

extract: setup ## Copy Lagos days into dw.raw: make extract DAY=2026-11-09 [LOOKBACK=2]
	$(PY) ingestion/batch/extract_to_dw.py --day $(DAY) --lookback $(or $(LOOKBACK),2)

extract-week: setup ## Copy the seven course days into dw.raw (09 and the six days before it)
	$(PY) ingestion/batch/extract_to_dw.py --day 2026-11-09 --lookback 6

check-w5: setup ## Week 5 checkpoints, headless
	$(PY) scripts/checks/check_w5.py
```

Also remove `check-w5` from the "arrives with its week" line.

### A3. The extract script

**Touch:** **new** `ingestion/batch/extract_to_dw.py`. Keep it short and readable, because the class reads it on screen in step 3:
- **Transactions:** for each Lagos day, in one database transaction, delete that day from `raw.transactions` and `COPY` it from `owopay`. It adds `_wat_day` and `_extracted_at` columns.
- **Lookback:** `--lookback 2` (the default) also reloads the two previous days, so late status changes land.
- **`customers`, `merchants`, `agents`:** truncated and reloaded whole.
- **Settings:** from `owopay.config.SETTINGS`. No Airflow imports. Exits non-zero on failure.

### A4. The dbt project, complete

**Touch (all new):**

```
dbt/
  dbt_project.yml          name owopay, profile owopay; staging → views, marts → tables
  profiles.yml             target dw; env_var('PG_HOST'), PG_PORT, PG_USER, PG_PASSWORD, DW_DB
  packages.yml             dbt-labs/dbt_utils [">=1.3.0", "<2.0.0"] (1.4.1 today)
                           metaplane/dbt_expectations [">=0.10.0", "<0.11.0"] (0.10.10 today)
  macros/generate_schema_name.sql   schemas named exactly staging, snapshots, marts
  models/staging/
    _sources.yml           source raw: transactions, customers, merchants, agents; freshness on _extracted_at
    _staging.yml           unique/not_null on every ID; accepted_values for channel and status;
                           dbt_expectations range on amount_kobo (minimum 1)
    stg_transactions.sql   renamed and typed, wat_day from _wat_day; no joins, no filters
    stg_customers.sql      no full_name, phone_number or bvn: personal data stops here
    stg_merchants.sql, stg_agents.sql
  snapshots/
    snap_customers.yml     ref('stg_customers'); strategy timestamp on updated_at, so each version
                           starts when the source says it changed; unique_key customer_id
  models/marts/core/
    dim_date.sql           generate_series 2025-01-01..2027-12-31; date_key yyyymmdd, day_name,
                           is_weekend, is_salary_window (day >= 25), month_start, year
    dim_customer.sql       from snap_customers; first version back-dated to 1900-01-01;
                           valid_to 9999-12-31 when current; is_current
    dim_merchant.sql, dim_agent.sql   Type 1, dbt_utils.generate_surrogate_key keys
    fct_transactions.sql   one row per transaction; date_key from wat_day; customer_key where
                           created_at falls in the version's window; left joins;
                           post_hook: create index on {{ this }} (date_key), no index name
    _core.yml              unique/not_null on keys; relationships from each fact key to its dim
  models/marts/finance/
    mart_daily_txn_summary.sql   one row per wat_day × channel × status: txns, amount_kobo,
                                 amount_naira = amount_kobo / 100.0, day_name, is_salary_window
  tests/
    assert_fct_reconciles_to_raw.sql   any Lagos day where the fact's count or kobo differs from raw
```

Two details that break silently:
- **Back-date the first customer version.** Otherwise transactions made before the first snapshot get no customer.
- **No name on the post-hook index.** A named index collides with the copy dbt keeps while it swaps tables.

Put a header comment of two or three lines at the top of every model, saying what it is and why. Those comments are your speaking notes in class.

### A5. `make check-w5`

**Touch:** **new** `scripts/checks/check_w5.py`, the same `Checks` pattern as `check_w4.py`. The checks follow the class steps, so the room sees them go green one by one:
1. `dw.raw` has 7 Lagos days, each matching `owopay` (step 3).
2. `dbt/target/run_results.json` from the last build has no errors or failures (steps 5–8).
3. `marts.fct_transactions` count equals `raw.transactions`, and the reconciliation test passed (step 6).
4. At least one customer has two versions in `marts.dim_customer` (step 7).
5. `marts.mart_daily_txn_summary` answers in under 1 second (step 8).
6. Lab files: `docs/adr/ADR-002-fact-grain.md` with a Decision section, and `docs/design/data-model.md` (part C).

### A6. Prove it from nothing, then push

Run exactly what the class will run, in order, from an empty stack:

```bash
make clean && make up PROFILE=core
make seed-week && make extract-week
make dbt ARGS="deps"
make dbt ARGS="build -s staging"
make dbt ARGS="build"
make seed DAY=2026-11-10 ROWS=100000 && make extract DAY=2026-11-10 && make dbt ARGS="build"
make check-w5          # 1–5 PASS, 6 "not written yet"
make lint && make test
```

Then:
1. Merge `week-05` into the template's `main` and push it, so learners can rebuild it after class.
2. Update the deck so slides 11, 12 and 14 show the real commands (`make extract`, `make dbt ARGS=…`).

### A7. Your laptop, the morning of class

What is already cached on the instructor's Mac, so the class needs no internet:
- the four core images;
- `.venv` with dbt 1.12.6;
- `dbt/dbt_packages` (dbt_utils, dbt_expectations, dbt_date);
- `.env` with `PG_PORT=5433`, clear of the Homebrew Postgres on 5432.

Keep all of those: don't delete `.venv` or `dbt/dbt_packages`.

```bash
# 1. Docker Desktop running, then free its CPU: Airbyte took ~8.5 of 8 cores in rehearsal
docker stop airbyte-abctl-control-plane          # docker start airbyte-abctl-control-plane after class

# 2. Empty the stack, so the room sees it come up from nothing (images, venv and packages stay)
cd ~/altschool/week4/owopay-platform
echo yes | make clean

# 3. Optional: one timed run of part B without Airbyte, then `echo yes | make clean` again
```

Projector checklist:
- terminal font large;
- the editor open on the repo, with the files of each step in the order of "Files at a glance";
- `make psql DB=dw` ready in a second terminal tab.

Tell learners the day before: bring one question OwoPay's warehouse must answer in under a second. Nothing to install for class.

---

## Part B · In class: run it, read it

Everything below runs on your laptop, projected. The rhythm for each step:
1. You give the idea in two minutes.
2. You run the commands.
3. While they run, or right after, you open the files and walk through them.
4. You show the checkpoint on screen.

Timings in each step are from the rehearsal on your Mac. The step times leave room for questions; the commands themselves take much less.

If a command fails in front of the room, read the first red line aloud. It is a teaching moment, and the fix is in the troubleshooting table.

| Clock | Step |
| --- | --- |
| 0:00 | Opening: Week 4's problem, today's picture (the star) |
| 0:10 | 1 · The platform up |
| 0:25 | 2 · OwoPay seeded, and the slow query |
| 0:35 | 3 · Extract into the warehouse |
| 0:47 | 4 · The dbt project |
| 0:55 | 5 · Sources and staging |
| 1:07 | 6 · The star |
| 1:22 | 7 · History |
| 1:34 | 8 · Under a second |
| 1:44 | 9 · Check, lineage, lab brief |
| 1:55 | Exit ticket |

### Step 1 · The platform up (0:10, 15 min)

**Idea:** Week 4's three stores, now running: Postgres (the record of truth), Redpanda (the queue), SeaweedFS (the filing cabinet).

**Run:**

```bash
make setup                 # prints "venv ready": already built; explain what it would install
make up PROFILE=core       # about 30 s
make ps
```

**Walk through:**
- `docker-compose.yml` and `compose/stack.yml`: the core profile, one service per Week 4 store, and the memory limits that fit an 8 GB laptop.
- `versions.env`: one place for every version, as ADR-000 promised.
- `.env.example`: every setting has a name, and nothing is hard-coded.

**Checkpoint:** `make ps` shows postgres, redpanda and seaweedfs healthy.

### Step 2 · OwoPay seeded, and the slow query (0:25, 10 min)

**Idea:**
- The data is deterministic: when learners rebuild it at home, their numbers match yours exactly.
- The business day is Lagos time, and money is kobo.

**Run:**

```bash
make seed-week             # 2–3 minutes with Airbyte stopped: the walk-through below fills it
make demo
make psql
```

```sql
select channel, status, count(*) from transactions group by 1, 2 order by 1, 2;
explain analyze
select date_trunc('day', created_at), status, count(*), sum(amount_kobo)
from transactions group by 1, 2;           -- Week 4's finance query: every row, every refresh
```

**Walk through** (while the seed runs):
- `docs/schema.md`: the five tables, the two topics, and the conventions box.
- `generator/owopay/day.py`: `day_bounds` (the Lagos day), `CHANNELS` and `TIER_LIMIT_NAIRA` (realistic shapes), and `customer_changes` (the history we model in step 7).

**Checkpoint:** `make demo` shows 800,000 transactions over 7 days.

### Step 3 · Extract into the warehouse (0:35, 12 min)

**Idea:** from today, analytics never touches `owopay`.

**Run:**

```bash
make extract-week                      # about 50 s: day 09 with a lookback of 6 days
make extract DAY=2026-11-09            # again, default lookback 2: days 07–09 "replaced", same counts
make psql DB=dw
```

```sql
select _wat_day, count(*), sum(amount_kobo) from raw.transactions group by 1 order by 1;
```

**Walk through** `ingestion/batch/extract_to_dw.py`, three things:
- delete-and-reload of each day inside one transaction: a rerun is safe, and a power cut leaves yesterday intact;
- the lookback, which picks up late settlements;
- the reference tables, copied whole.

**Checkpoint:** seven days, the same counts after the second run (check 1 when you run `make check-w5` in step 9).

### Step 4 · The dbt project (0:47, 8 min)

**Idea:** dbt turns `select` statements into tables, in dependency order, with tests.

**Run:**

```bash
make dbt ARGS="debug"      # All checks passed!
make dbt ARGS="ls"         # every model, snapshot and test it found
```

Don't run `make dbt ARGS="deps"` in class. The packages are already installed, and `deps` downloads them again, which needs the internet. Show `packages.yml` and say what `deps` does: learners run it at home.

**Walk through:**
- `dbt/dbt_project.yml`: folders become schemas, views versus tables.
- `dbt/profiles.yml`: the connection comes from `.env`, which is why we run dbt through `make dbt`.
- `dbt/packages.yml`: dbt_utils and dbt_expectations.

**Checkpoint:** `debug` passes.

### Step 5 · Sources and staging (0:55, 12 min)

**Idea:**
- Staging is one model per source table: renamed and typed, nothing else.
- Personal data stops here.
- Tests are the contract.

**Run:**

```bash
make dbt ARGS="build -s staging"     # about 10 s, 19 of 19 green
make psql DB=dw
```

```sql
select * from staging.stg_customers limit 3;   -- no name, phone or BVN
```

**Walk through:**
- `dbt/models/staging/_sources.yml`: `raw` declared once, with freshness.
- `stg_transactions.sql`: rename and type, nothing more.
- `stg_customers.sql`: the columns that are *not* there.
- `_staging.yml`: each test, and what it would catch.

**Checkpoint:** every model and test passes.

### Step 6 · The star (1:07, 15 min)

**Idea:**
- Say the grain sentence out loud: one row per transaction, in its latest status.
- Facts and dimensions.
- Surrogate keys.

**Run:**

```bash
make dbt ARGS="build"      # 48 nodes in about 80 seconds: snapshot, dims, fact, mart, every test
make psql DB=dw
```

```sql
select count(*) from marts.fct_transactions;   -- 800,000
```

dbt prints one warning on the snapshot (`DATETIME` vs `DATETIMETZ`). It is harmless: every column involved is `timestamptz`. Say so before someone asks.

**Walk through:**
- `fct_transactions.sql`: the grain comment, the keys, the left joins, the post-hook index.
- `dim_merchant.sql`: a Type 1 dimension in five lines.
- `_core.yml`: `relationships`.
- `tests/assert_fct_reconciles_to_raw.sql`: the regulator's "to the kobo" as a query that must return nothing.
- `dbt/target/run_results.json`: open it once, so the room sees what dbt records.

**Checkpoint:** all green, including the reconciliation test; 800,000 rows in the fact.

### Step 7 · History (1:22, 12 min)

**Idea:** customers change, and the regulator wants each transaction's KYC tier at the time it happened.

**Run:**

```bash
make seed DAY=2026-11-10 ROWS=100000      # tomorrow: ~200 customers change (about 45 s)
make extract DAY=2026-11-10               # about 20 s
make dbt ARGS="build"                     # about 80 s
make psql DB=dw
```

```sql
select customer_id, version_number, kyc_tier, state, valid_from, valid_to, is_current
from marts.dim_customer
where customer_id in (select customer_id from marts.dim_customer group by 1 having count(*) > 1 limit 1)
order by version_number;

-- the point of it: each transaction kept the version current when the money moved
select c.version_number, c.state, count(*)
from marts.fct_transactions f join marts.dim_customer c using (customer_key)
where c.customer_id = '<the customer above>'
group by 1, 2;
```

In rehearsal, exactly 200 customers changed on day 10 and the snapshot inserted exactly 200 versions. CUS-000351 moved from Ogun to Lagos: 64 transactions stayed on the Ogun version and 8 landed on the Lagos one.

**Walk through:**
- `dbt/snapshots/snap_customers.yml`: the timestamp strategy. The source stamps `updated_at` when a customer changes, so each version starts at the real moment of change, not when dbt happened to run. History starts the first time the snapshot runs.
- `dim_customer.sql`: the back-dated first version and the validity window.
- The join in `fct_transactions.sql`: `created_at` inside the window, so a transfer made before the change keeps the old tier forever.

**Checkpoint:** one customer, two rows (check 4 in step 9).

### Step 8 · Under a second (1:34, 10 min)

**Idea:** the plan is the proof, not the stopwatch. Rows read is what grows to a billion.

**Run:**

```bash
make psql DB=dw
```

```sql
explain analyze select wat_day, status, sum(txns), sum(amount_naira)
from marts.mart_daily_txn_summary group by 1, 2;     -- about 100 rows, against every transaction in step 2
```

Rehearsal numbers, after step 7's extra day: Week 4's query on `owopay` scanned 900,000 rows in 3.6 s; the mart read 120 rows in 1.4 ms.

**Walk through:**
- `dbt/models/marts/finance/mart_daily_txn_summary.sql`: the group-by happens once, at build time.
- Kobo stays an integer until the last column.

**Checkpoint:** both plans side by side (check 5 in step 9).

### Step 9 · Check, lineage, lab brief (1:44, 11 min)

**Run:**

```bash
make check-w5              # 1–5 PASS; 6 fails until the lab is done
make dbt ARGS="docs generate"
.venv/bin/dbt docs serve --project-dir dbt --profiles-dir dbt --port 8085   # open http://localhost:8085
```

**Walk through:** the `make check-w5` output first, one check per step of the class. Then the lineage graph, from `raw` to the mart: everything run today, in one picture. Then brief part C, starting with its first row: rebuild what they just watched.

---

## Part C · Lab (due Day 7, 23:59): the learners' turn to write code

| Do | Touch | Prove with |
| --- | --- | --- |
| **First: rebuild what you watched.** On GitHub, **Use this template** → your own repo. On your laptop (`make doctor` first) or in a Codespace: `make setup`, `make up PROFILE=core`, `make seed-week`, `make extract-week`, `make dbt ARGS="deps"`, `make dbt ARGS="build"`. Add `git remote add upstream <template URL>` to pull each week's code | nothing: the template has it all | `make check-w5` checks 1–3 and 5; your numbers match the class's |
| History, as in step 7 | `make seed DAY=2026-11-10 ROWS=100000`, `make extract DAY=2026-11-10`, `make dbt ARGS="build"` | `make check-w5` check 4 |
| Your own question as a mart, with at least two tests | **new** `dbt/models/marts/<consumer>/mart_<name>.sql` and `_<consumer>.yml` | `make dbt ARGS="build -s mart_<name>"`; `explain analyze` under 1 s, pasted in the PR |
| Regulator T+1 mart: one row per Lagos day, counts and kobo by status, reconciled to raw | **new** `dbt/models/marts/regulator/mart_regulator_t1.sql` and a singular test | `make dbt ARGS="build -s mart_regulator_t1+"` |
| ADR-002: the grain of `fct_transactions`, three options, why two lost | **new** `docs/adr/ADR-002-fact-grain.md` | `make check-w5` check 6 |
| Data model: ERD, grain sentence, Type 1/2 per dimension, conventions (kobo, Lagos day, personal data) | **new** `docs/design/data-model.md` | review |
| Week 4 backlog: capacity model, ADR-001, System Design Doc v0 with its container diagram | run `notebooks/week04_systems_thinking.ipynb` sections 4–5 on the seeded stack | `make check-w4` stages 4–5 |
| Stretch: an "unknown agent" row so `agent_key` is never empty | `dim_agent.sql`, `fct_transactions.sql` | `relationships` plus `not_null` on `agent_key` |

The PR includes the lineage screenshot from step 9.

**Accepted when:**
- `make check-w5` is green after a rebuild from a fresh clone, on a laptop or in a Codespace;
- every new model has a test;
- the grain is stated in one sentence;
- the reconciliation test passes.

---

## Week 4 backlog: where each piece goes

| Week 4 item | Goes to |
| --- | --- |
| Stack up, seed, tour | Shown in class (steps 1–2); learners do it in the lab's first row |
| Capacity model, ADR-001, System Design Doc v0 | Week 5 lab, from the Week 4 notebook on the seeded stack |
| Bridge lab: own DAG and producer | Week 6, when Airflow starts (`make check-w4` stage 3 stays red until then) |
| Local Docker and `make doctor` | The lab's first row; Codespaces is the fallback |
| 300-word case study | Week 5 PR, if it was not posted |

## What Week 6 relies on (do not rename)

- **Extract:** `python ingestion/batch/extract_to_dw.py --day YYYY-MM-DD --lookback N` exits non-zero on failure.
- **`dbt build`:** succeeds from the repo root through `make dbt`.
- **Model names:** `stg_transactions`, `snap_customers`, `dim_date`, `dim_customer`, `dim_merchant`, `dim_agent`, `fct_transactions`, `mart_daily_txn_summary`.
- **Schemas in `dw`:** `raw`, `staging`, `snapshots`, `marts`.

## Files at a glance

```
Written before class and pushed to the template (part A)
  pyproject.toml, Makefile                  changed   dbt; include .env; SSL_CERT_FILE;
                                                      dbt, seed-week, extract, extract-week, check-w5
  ingestion/batch/extract_to_dw.py          new
  dbt/                                      new       the whole project: staging, snapshot, star, mart, tests
  scripts/checks/check_w5.py                new

Walked through in class (part B), in this order
  compose/stack.yml, versions.env, .env.example          step 1
  docs/schema.md, generator/owopay/day.py                step 2
  ingestion/batch/extract_to_dw.py                       step 3
  dbt/dbt_project.yml, profiles.yml, packages.yml        step 4
  dbt/models/staging/*                                   step 5
  dbt/models/marts/core/fct_transactions.sql, dim_merchant.sql, _core.yml,
  dbt/tests/assert_fct_reconciles_to_raw.sql             step 6
  dbt/snapshots/snap_customers.yml, dim_customer.sql     step 7
  dbt/models/marts/finance/mart_daily_txn_summary.sql    step 8

Written by learners (part C)
  dbt/models/marts/<consumer>/, dbt/models/marts/regulator/
  docs/adr/ADR-002-fact-grain.md, docs/design/data-model.md
```

## When something goes wrong

In class it is your laptop; in the lab it is theirs. Same table.

| Symptom | Cause | Fix |
| --- | --- | --- |
| `make up` fails: Cannot connect to the Docker daemon | Docker Desktop not started | start it, wait for "Engine running", run `make up` again |
| Everything is several times slower than the timings here | another container is using Docker's CPU (`docker stats`); on your Mac, Airbyte | `docker stop` it for the class |
| `dbt debug`: role "owopay" does not exist | dbt reached a local Postgres on 5432 | `PG_PORT=5433` in `.env`, and always run dbt through `make dbt` |
| A learner's Codespace is slow to start or `make up` stalls | first-time setup, or a 2-core Codespace | wait for the first build; recreate it as 4-core |
| `pip` fails on `dbt-core-experimental-parser` with `CERTIFICATE_VERIFY_FAILED` (Mac laptop) | python.org Python has no CA bundle | `make setup` handles it; by hand, run "Install Certificates.command" in the Python folder |
| `dbt deps` hangs | network to hub.getdbt.com | retry on a better connection; never needed in class (A7) |
| Reconciliation test fails after a reseed | `raw` not refreshed | `make extract DAY=… LOOKBACK=2`, then `make dbt ARGS="build"` |
| dbt warns `Data type of snapshot table timestamp columns (DATETIME) doesn't match … (DATETIMETZ)` | dbt's type check, not the data | ignore it; every column involved is `timestamptz` |
| A learner's rebuild fails in the lab | anything | the first red line, this table, then the cohort channel with the full output |
