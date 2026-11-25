"""make check-w6: the Week 6 checkpoints, in the order the class reaches them."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from check_w5 import fact_reconciles, history_kept, lab_file  # noqa: E402

from common import REPO_ROOT, SETTINGS, Checks, compose, pg, running_services  # noqa: E402

DAG_ID = 'owopay_daily'
LAGOS_DAY = "(created_at at time zone 'Africa/Lagos')::date"


def macros_used():
    """The macros rendered into the compiled SQL, and what they produced agrees on both sides."""
    compiled = REPO_ROOT / 'dbt' / 'target' / 'compiled' / 'owopay' / 'models' / 'marts'
    fct = compiled / 'core' / 'fct_transactions.sql'
    if not fct.exists():
        raise RuntimeError('fct_transactions has not been compiled: run dbt build')
    if "to_char(t.wat_day, 'YYYYMMDD')::int" not in fct.read_text():
        raise RuntimeError('the date_key macro did not render into fct_transactions')
    with pg(SETTINGS['DW_DB']) as conn:
        bad_naira = conn.execute(
            'select count(*) from marts.mart_daily_txn_summary where amount_naira <> amount_kobo / 100.0'
        ).fetchone()[0]
        orphan_days = conn.execute(
            'select count(*) from (select distinct date_key from marts.fct_transactions) f '
            'left join marts.dim_date d using (date_key) where d.date_key is null'
        ).fetchone()[0]
    if bad_naira or orphan_days:
        raise RuntimeError('%d wrong naira rows, %d fact days not in dim_date' % (bad_naira, orphan_days))
    return 'date_key rendered; every naira and every fact day agree'


def airflow_ready():
    services = running_services()
    if SETTINGS['AIRFLOW_SERVICE'] not in services:
        raise RuntimeError('Airflow is not running')
    errors = json.loads(
        compose('exec', '-T', SETTINGS['AIRFLOW_SERVICE'], 'airflow', 'dags', 'list-import-errors', '-o', 'json')
        or '[]'
    )
    if errors:
        raise RuntimeError('import errors: %s' % [e.get('filepath') for e in errors])
    dags = json.loads(compose('exec', '-T', SETTINGS['AIRFLOW_SERVICE'], 'airflow', 'dags', 'list', '-o', 'json'))
    if DAG_ID not in {d['dag_id'] for d in dags}:
        raise RuntimeError('%s is not loaded' % DAG_ID)
    return '%d DAGs, no import errors' % len(dags)


def last_run():
    with pg('airflow') as conn:
        row = conn.execute(
            'select state, conf, run_id from dag_run where dag_id = %s order by start_date desc nulls last limit 1',
            (DAG_ID,),
        ).fetchone()
    if not row:
        raise RuntimeError('%s has never run' % DAG_ID)
    state, conf, run_id = row
    if state != 'success':
        raise RuntimeError('last run %s is %s' % (run_id, state))
    return 'last run succeeded (day %s)' % ((conf or {}).get('day') or 'yesterday')


def day_loaded():
    """The newest Lagos day in owopay is in raw, row for row: the DAG moved it, not a person."""
    with pg() as src:
        day, n = src.execute(
            'select %s, count(*) from transactions group by 1 order by 1 desc limit 1' % LAGOS_DAY
        ).fetchone()
    with pg(SETTINGS['DW_DB']) as conn:
        raw = conn.execute('select count(*) from raw.transactions where _wat_day = %s', (day,)).fetchone()[0]
    if raw != n:
        raise RuntimeError('%s: owopay has %s rows, raw has %s' % (day, n, raw))
    return '%s: %s rows in owopay and raw' % (day, format(n, ','))


if __name__ == '__main__':
    c = Checks('make check-w6')
    c.stage('In class · dbt')
    c.run('1 Macros', macros_used, 'make dbt ARGS="build" (part 1)')
    c.run('2 Fact reconciles', fact_reconciles, 'make dbt ARGS="build"')
    c.stage('In class · Airflow')
    c.run('3 Airflow up', airflow_ready, 'make up PROFILE=orchestration, then make logs SERVICE=airflow-dag-processor')
    c.run('4 owopay_daily ran', last_run, 'make trigger DAY=2026-11-11 and read the failed task log in the UI')
    c.run('5 Newest day loaded', day_loaded, 'make seed DAY=2026-11-11 ROWS=100000, then make trigger DAY=2026-11-11')
    c.run('6 History', history_kept, 'the DAG run for a new day builds the snapshot: a second version appears')
    c.stage('Lab')
    c.run('7 Runbook', lab_file('docs/runbooks/owopay_daily.md', 'extract'), 'the lab: write the runbook')
    c.finish()
