"""make check-w5: the Week 5 checkpoints, in the order the class reaches them."""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import REPO_ROOT, SETTINGS, Checks, pg  # noqa: E402

LAGOS_DAY = "(created_at at time zone 'Africa/Lagos')::date"
COURSE_DAYS = 7


def dw():
    return pg(SETTINGS['DW_DB'])


def raw_loaded():
    with pg() as src:
        source = dict(src.execute('select %s, count(*) from transactions group by 1' % LAGOS_DAY).fetchall())
    with dw() as conn:
        raw = dict(conn.execute('select _wat_day, count(*) from raw.transactions group by 1').fetchall())
    if len(raw) < COURSE_DAYS:
        raise RuntimeError('raw.transactions has %d Lagos day(s); expected at least %d' % (len(raw), COURSE_DAYS))
    stale = sorted(str(d) for d, n in raw.items() if source.get(d) != n)
    if stale:
        raise RuntimeError('raw differs from owopay on: %s' % ', '.join(stale))
    return '%d days, %s rows, each matching owopay' % (len(raw), format(sum(raw.values()), ','))


def last_build():
    results = REPO_ROOT / 'dbt' / 'target' / 'run_results.json'
    if not results.exists():
        raise RuntimeError('dbt has not run yet')
    run = json.loads(results.read_text())
    bad = [r['unique_id'] for r in run['results'] if r['status'] in ('error', 'fail', 'runtime error')]
    if bad:
        raise RuntimeError('%d failed: %s' % (len(bad), ', '.join(bad[:5])))
    return '%d nodes OK in the last `%s`' % (len(run['results']), ' '.join(run['args'].get('which', 'dbt').split()))


def fact_reconciles():
    with dw() as conn:
        raw, raw_kobo = conn.execute('select count(*), sum(amount_kobo) from raw.transactions').fetchone()
        fct, fct_kobo = conn.execute('select count(*), sum(amount_kobo) from marts.fct_transactions').fetchone()
    if (raw, raw_kobo) != (fct, fct_kobo):
        raise RuntimeError('raw %s rows / %s kobo, fact %s rows / %s kobo' % (raw, raw_kobo, fct, fct_kobo))
    return '%s transactions and %s kobo in both' % (format(fct, ','), format(fct_kobo, ','))


def history_kept():
    with dw() as conn:
        n = conn.execute("""select count(*) from (select customer_id from marts.dim_customer
                            group by 1 having count(*) > 1) v""").fetchone()[0]
    if not n:
        raise RuntimeError('no customer has a second version yet: seed and extract a new day, then dbt build')
    return '%s customers with more than one version' % format(n, ',')


def mart_fast():
    with dw() as conn:
        t0 = time.perf_counter()
        rows = conn.execute("""select wat_day, status, sum(txns), sum(amount_naira)
                               from marts.mart_daily_txn_summary group by 1, 2""").fetchall()
        ms = (time.perf_counter() - t0) * 1000
    if ms >= 1000:
        raise RuntimeError('%.0f ms' % ms)
    return '%d rows in %.0f ms' % (len(rows), ms)


def lab_file(path, must_contain):
    def check():
        if not (REPO_ROOT / path).exists():
            raise RuntimeError('%s is not written yet' % path)
        if must_contain not in (REPO_ROOT / path).read_text():
            raise RuntimeError('%s has no "%s" section' % (path, must_contain))
        return path

    return check


def load_audit_matches():
    expected, bad = set(), []
    with dw() as conn:
        for day, rows, kobo in conn.execute(
            'select _wat_day::text, count(*), sum(amount_kobo)::bigint from raw.transactions group by 1'
        ):
            expected.add(('transactions', day))
            audit = conn.execute(
                'select row_count, kobo from raw.load_audit where dataset = %s and partition_key = %s',
                ('transactions', day),
            ).fetchone()
            if audit != (rows, kobo):
                bad.append('%s: audit %s, raw %s' % (day, audit, (rows, kobo)))
        for table in ('customers', 'merchants', 'agents'):
            expected.add((table, 'all'))
            rows = conn.execute('select count(*) from raw.%s' % table).fetchone()[0]
            audit = conn.execute(
                "select row_count from raw.load_audit where dataset = %s and partition_key = 'all'", (table,)
            ).fetchone()
            if audit != (rows,):
                bad.append('%s: audit %s, raw %s' % (table, audit, rows))
        actual = set(conn.execute('select dataset, partition_key from raw.load_audit').fetchall())
        if actual != expected:
            bad.append('partitions: audit %s, expected %s' % (sorted(actual), sorted(expected)))
    if bad:
        raise RuntimeError('; '.join(bad[:3]))
    return '%d partitions match raw' % len(expected)


def raw_keys_unique():
    checks = [
        ('transactions', 'txn_id'),
        ('customers', 'customer_id'),
        ('merchants', 'merchant_id'),
        ('agents', 'agent_id'),
    ]
    bad = []
    with dw() as conn:
        for table, key in checks:
            nulls = conn.execute('select count(*) from raw.%s where %s is null' % (table, key)).fetchone()[0]
            dupes = conn.execute(
                'select count(*) from (select %s from raw.%s group by 1 having count(*) > 1) d' % (key, table)
            ).fetchone()[0]
            if nulls or dupes:
                bad.append('%s.%s: %s null, %s duplicate' % (table, key, nulls, dupes))
    if bad:
        raise RuntimeError('; '.join(bad))
    return '4 raw keys unique and not null'


if __name__ == '__main__':
    c = Checks('make check-w5')
    c.stage('In class')
    c.run('1 Extract', raw_loaded, 'make extract-week (step 3)')
    c.run('2 dbt build', last_build, 'make dbt ARGS="build" and read the first red line (steps 5-8)')
    c.run('3 Fact reconciles', fact_reconciles, 'make dbt ARGS="build" after every extract (step 6)')
    c.run('4 History', history_kept, 'make seed DAY=2026-11-10 ROWS=100000, make extract DAY=2026-11-10, dbt build')
    c.run('5 Under a second', mart_fast, 'make dbt ARGS="build -s mart_daily_txn_summary" (step 8)')
    c.stage('Lab')
    c.run('6 ADR-002', lab_file('docs/adr/ADR-002-fact-grain.md', 'Decision'), 'the lab: write ADR-002')
    c.run('6 Data model', lab_file('docs/design/data-model.md', 'grain'), 'the lab: write the data-model doc')
    c.stage('Optional')
    c.run('7 Load ledger', load_audit_matches, 'make extract DAY=2026-11-10 and make dbt ARGS="build"')
    c.run('8 Raw keys', raw_keys_unique, 'remove duplicate or null keys, then make extract DAY=2026-11-10')
    c.finish()
