"""Copy OwoPay from the OLTP database (owopay) into the warehouse's raw schema (dw.raw).

    python ingestion/batch/extract_to_dw.py --day 2026-11-09 [--lookback 2]

From Week 5, analytics never reads owopay directly. This script copies a slice into dw.raw and dbt
models it there.

- Transactions move one Lagos business day at a time. Each day is deleted and reloaded inside one
  database transaction: a rerun replaces the day instead of duplicating it, and a failure halfway
  (a power cut, a dropped connection) leaves the previous load exactly as it was.
- --lookback also reloads the days before: a transfer started at 23:58 can settle after midnight,
  and only a reload picks up its final status.
- customers, merchants and agents are small, so they are copied whole on every run. Keeping their
  history is the dbt snapshot's job, not this script's.
- If --day itself has no transactions in owopay, it fails and rolls back. An empty load that "succeeds"
  is the worst failure: the dashboard shows zero and nobody is told. --allow-empty overrides it.
- The database enforces the invariant: txn_id is unique, so a duplicate fails the COPY and the whole
  run rolls back. A transaction-scoped advisory lock means only one extract writes at a time.
- Every successful load is upserted into raw.load_audit (run_id, rows, kobo, loaded_at), so a rerun is
  provable rather than inferred from _extracted_at, which changes on every run.

Week 6 runs this unchanged as the first task of the owopay_daily DAG.
"""

import argparse
import datetime as dt
import sys
import uuid
from pathlib import Path

import psycopg
from psycopg import sql

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'generator'))

from owopay.config import SETTINGS, pg_conninfo  # noqa: E402
from owopay.day import day_bounds  # noqa: E402

# raw mirrors the source columns, plus when we loaded each row (and, for transactions, which Lagos day).
# The unique indexes are the database's copy of the idempotency promise: a duplicate key fails the
# COPY and rolls the whole run back. load_audit keeps one row per dataset and partition.
RAW_DDL = """
create schema if not exists raw;

create table if not exists raw.customers (
    customer_id text not null, full_name text, phone_number text, bvn text, state text, kyc_tier smallint,
    signup_channel text, created_at timestamptz, updated_at timestamptz,
    _extracted_at timestamptz not null
);
create table if not exists raw.merchants (
    merchant_id text not null, business_name text, category text, state text, created_at timestamptz,
    _extracted_at timestamptz not null
);
create table if not exists raw.agents (
    agent_id text not null, business_name text, state text, created_at timestamptz,
    _extracted_at timestamptz not null
);
create table if not exists raw.transactions (
    txn_id uuid not null, customer_id text, merchant_id text, agent_id text, channel text, amount_kobo bigint,
    status text, created_at timestamptz,
    _wat_day date not null,
    _extracted_at timestamptz not null
);
create index if not exists transactions_wat_day_idx on raw.transactions (_wat_day);

-- Apply the keys to tables created before this change as well.
alter table raw.customers    alter column customer_id set not null;
alter table raw.merchants    alter column merchant_id set not null;
alter table raw.agents       alter column agent_id    set not null;
alter table raw.transactions alter column txn_id      set not null;

create unique index if not exists customers_customer_id_uidx on raw.customers (customer_id);
create unique index if not exists merchants_merchant_id_uidx on raw.merchants (merchant_id);
create unique index if not exists agents_agent_id_uidx       on raw.agents (agent_id);
create unique index if not exists transactions_txn_id_uidx   on raw.transactions (txn_id);

create table if not exists raw.load_audit (
    dataset       text        not null,
    partition_key text        not null,
    run_id        uuid        not null,
    row_count     bigint      not null,
    kobo          bigint,
    status        text        not null,
    loaded_at     timestamptz not null,
    primary key (dataset, partition_key)
);
"""

REFERENCE_TABLES = {
    'customers': [
        'customer_id',
        'full_name',
        'phone_number',
        'bvn',
        'state',
        'kyc_tier',
        'signup_channel',
        'created_at',
        'updated_at',
    ],
    'merchants': ['merchant_id', 'business_name', 'category', 'state', 'created_at'],
    'agents': ['agent_id', 'business_name', 'state', 'created_at'],
}
TRANSACTION_COLUMNS = [
    'txn_id',
    'customer_id',
    'merchant_id',
    'agent_id',
    'channel',
    'amount_kobo',
    'status',
    'created_at',
]


def columns(names):
    return sql.SQL(', ').join(map(sql.Identifier, names))


def stream(src, dst, select, table, names):
    """Pipe the rows of a select on owopay straight into a dw.raw table with COPY, never holding them in memory."""
    copy_out = sql.SQL('copy ({}) to stdout').format(select)
    copy_in = sql.SQL('copy {} ({}) from stdin').format(sql.Identifier('raw', table), columns(names))
    with src.cursor().copy(copy_out) as out, dst.cursor().copy(copy_in) as into:
        for block in out:
            into.write(block)


def extract_reference(src, dst, table, names, loaded_at):
    dst.execute(sql.SQL('truncate {}').format(sql.Identifier('raw', table)))
    select = sql.SQL('select {}, {} from {}').format(columns(names), sql.Literal(loaded_at), sql.Identifier(table))
    stream(src, dst, select, table, names + ['_extracted_at'])
    return dst.execute(sql.SQL('select count(*) from {}').format(sql.Identifier('raw', table))).fetchone()[0]


def extract_day(src, dst, day, loaded_at):
    start, end = day_bounds(day)  # the Lagos day as a UTC range: 23:00 the day before to 23:00
    replaced = dst.execute('delete from raw.transactions where _wat_day = %s', (day,)).rowcount
    select = sql.SQL('select {}, {}, {} from transactions where created_at >= {} and created_at < {}').format(
        columns(TRANSACTION_COLUMNS), sql.Literal(day), sql.Literal(loaded_at), sql.Literal(start), sql.Literal(end)
    )
    stream(src, dst, select, 'transactions', TRANSACTION_COLUMNS + ['_wat_day', '_extracted_at'])
    loaded, kobo = dst.execute(
        'select count(*), sum(amount_kobo)::bigint from raw.transactions where _wat_day = %s', (day,)
    ).fetchone()
    return replaced, loaded, kobo


def record_load(dst, dataset, partition_key, run_id, row_count, kobo, loaded_at):
    """Upsert the latest successful load for one dataset and partition, inside the same transaction."""
    dst.execute(
        """
        insert into raw.load_audit (dataset, partition_key, run_id, row_count, kobo, status, loaded_at)
        values (%s, %s, %s, %s, %s, 'success', %s)
        on conflict (dataset, partition_key) do update
           set run_id = excluded.run_id,
               row_count = excluded.row_count,
               kobo = excluded.kobo,
               status = excluded.status,
               loaded_at = excluded.loaded_at
        """,
        (dataset, partition_key, run_id, row_count, kobo, loaded_at),
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description='Copy OwoPay Lagos days from owopay into dw.raw')
    parser.add_argument('--day', required=True, type=dt.date.fromisoformat, help='Lagos business day, YYYY-MM-DD')
    parser.add_argument('--lookback', type=int, default=2, help='also reload this many days before --day')
    parser.add_argument('--allow-empty', action='store_true', help='succeed even if --day has no transactions')
    args = parser.parse_args(argv)

    days = [args.day - dt.timedelta(days=n) for n in range(args.lookback, -1, -1)]
    loaded_at = dt.datetime.now(dt.timezone.utc)
    run_id = uuid.uuid4()

    # Two connections: read from owopay, write to dw. Leaving the `with` commits dw once, at the end;
    # any error before that rolls the whole run back.
    with psycopg.connect(**pg_conninfo()) as src, psycopg.connect(**pg_conninfo(SETTINGS['DW_DB'])) as dst:
        # One writer at a time: the lock is held until this transaction commits or rolls back.
        dst.execute("select pg_advisory_xact_lock(hashtext('owopay.extract')::bigint)")
        dst.execute(RAW_DDL)
        for table, names in REFERENCE_TABLES.items():
            n = extract_reference(src, dst, table, names, loaded_at)
            record_load(dst, table, 'all', run_id, n, None, loaded_at)
            print('[extract] raw.%-13s %10s rows (full copy)' % (table, format(n, ',')))
        loaded = 0
        for day in days:
            replaced, loaded, kobo = extract_day(src, dst, day, loaded_at)
            record_load(dst, 'transactions', day.isoformat(), run_id, loaded, kobo, loaded_at)
            note = ' (replaced %s)' % format(replaced, ',') if replaced else ''
            print('[extract] raw.transactions %s %10s rows%s' % (day, format(loaded, ','), note))
        if not loaded and not args.allow_empty:
            # Raising inside the `with` rolls back every day of this run, so raw stays as it was.
            sys.exit('[extract] owopay has no transactions for %s yet: nothing committed' % args.day)


if __name__ == '__main__':
    main()
