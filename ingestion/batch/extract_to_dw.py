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

Week 6 runs this unchanged as the first task of the owopay_daily DAG.
"""

import argparse
import datetime as dt
import sys
from pathlib import Path

import psycopg
from psycopg import sql

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'generator'))

from owopay.config import SETTINGS, pg_conninfo  # noqa: E402
from owopay.day import day_bounds  # noqa: E402

# raw mirrors the source columns, plus when we loaded each row (and, for transactions, which Lagos day).
RAW_DDL = """
create schema if not exists raw;

create table if not exists raw.customers (
    customer_id text, full_name text, phone_number text, bvn text, state text, kyc_tier smallint,
    signup_channel text, created_at timestamptz, updated_at timestamptz,
    _extracted_at timestamptz not null
);
create table if not exists raw.merchants (
    merchant_id text, business_name text, category text, state text, created_at timestamptz,
    _extracted_at timestamptz not null
);
create table if not exists raw.agents (
    agent_id text, business_name text, state text, created_at timestamptz,
    _extracted_at timestamptz not null
);
create table if not exists raw.transactions (
    txn_id uuid, customer_id text, merchant_id text, agent_id text, channel text, amount_kobo bigint,
    status text, created_at timestamptz,
    _wat_day date not null,
    _extracted_at timestamptz not null
);
create index if not exists transactions_wat_day_idx on raw.transactions (_wat_day);
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
    loaded = dst.execute('select count(*) from raw.transactions where _wat_day = %s', (day,)).fetchone()[0]
    return replaced, loaded


def main(argv=None):
    parser = argparse.ArgumentParser(description='Copy OwoPay Lagos days from owopay into dw.raw')
    parser.add_argument('--day', required=True, type=dt.date.fromisoformat, help='Lagos business day, YYYY-MM-DD')
    parser.add_argument('--lookback', type=int, default=2, help='also reload this many days before --day')
    args = parser.parse_args(argv)

    days = [args.day - dt.timedelta(days=n) for n in range(args.lookback, -1, -1)]
    loaded_at = dt.datetime.now(dt.timezone.utc)

    # Two connections: read from owopay, write to dw. Leaving the `with` commits dw once, at the end;
    # any error before that rolls the whole run back.
    with psycopg.connect(**pg_conninfo()) as src, psycopg.connect(**pg_conninfo(SETTINGS['DW_DB'])) as dst:
        dst.execute(RAW_DDL)
        for table, names in REFERENCE_TABLES.items():
            n = extract_reference(src, dst, table, names, loaded_at)
            print('[extract] raw.%-13s %10s rows (full copy)' % (table, format(n, ',')))
        for day in days:
            replaced, loaded = extract_day(src, dst, day, loaded_at)
            note = ' (replaced %s)' % format(replaced, ',') if replaced else ''
            print('[extract] raw.transactions %s %10s rows%s' % (day, format(loaded, ','), note))


if __name__ == '__main__':
    main()
