"""owopay_daily: every morning, load yesterday's Lagos day into the warehouse and rebuild the model.

    resolve_day  ->  extract  ->  dbt_build

The DAG holds no business logic. extract runs ingestion/batch/extract_to_dw.py and dbt_build runs
the dbt project, exactly the commands we ran by hand with make. Airflow adds the schedule, the
retries, the order, one run at a time, and a log of every attempt.

Run one day by hand:  make trigger DAY=2026-11-11
"""

import datetime as dt

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

LAGOS = 'Africa/Lagos'
REPO = '/opt/owopay'  # the repo folders, mounted read-only (compose/stack.yml)
VENV = '/opt/owopay-venv/bin'  # dbt and psycopg 3, baked into the image (airflow/Dockerfile)
DBT = (
    f'{VENV}/dbt build --project-dir {REPO}/dbt --profiles-dir {REPO}/dbt'
    # The repo is read-only in here, so dbt writes its target/ and logs/ to the container's /tmp.
    ' --target-path /tmp/dbt/target --log-path /tmp/dbt/logs'
)


@dag(
    # 06:00 Lagos: the day closed at midnight, and late settlements have had six hours to land.
    schedule='0 6 * * *',
    start_date=pendulum.datetime(2026, 11, 1, tz=LAGOS),
    catchup=False,
    # The extract deletes and reloads days and dbt swaps tables: two runs at once would collide.
    max_active_runs=1,
    params={'day': Param('', type='string', description='Lagos day YYYY-MM-DD; empty means yesterday')},
    default_args={
        # Data that arrives late is the common failure, so retry. Production waits longer.
        'retries': 2,
        'retry_delay': dt.timedelta(minutes=1),
    },
    tags=['owopay', 'week6', 'maintainer: mubaraq.sani@needletech.ng'],
)
def owopay_daily():
    @task
    def resolve_day() -> str:
        """The Lagos day this run loads: the one asked for, or yesterday in Lagos."""
        ctx = get_current_context()
        if ctx['params'].get('day'):
            return dt.date.fromisoformat(ctx['params']['day']).isoformat()
        run_at = ctx.get('logical_date') or pendulum.now(LAGOS)
        return (run_at.in_timezone(LAGOS).date() - dt.timedelta(days=1)).isoformat()

    @task.bash
    def extract(day: str) -> str:
        """Copy the day, and the two before it, from owopay into dw.raw. Fails if the day is empty."""
        return f'{VENV}/python {REPO}/ingestion/batch/extract_to_dw.py --day {day} --lookback 2'

    # A failed dbt test is a data problem: retrying won't fix it, a person must look. So no retries.
    @task.bash(retries=0)
    def dbt_build() -> str:
        """Snapshot, models and every test, in dependency order: the same `dbt build` as in class."""
        return DBT

    extract(resolve_day()) >> dbt_build()


owopay_daily()