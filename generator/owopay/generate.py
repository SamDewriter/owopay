"""Seed one Lagos business day of OwoPay into the course stack.

    python -m owopay.generate --day 2026-11-03 --rows 200000 [--chaos duplicates,late,bad_records]

Deterministic per seed: everyone who runs the same day and row count gets the same rows and events.
Safe to rerun: the day's transactions are replaced, never duplicated. Events are appended to the
topics, so a rerun publishes the day's events a second time; that is what a replay looks like.
"""

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

from . import day as gen
from . import reference as ref
from .config import SETTINGS, pg_conninfo, s3_client

TOPICS = {'txn.events': 3, 'app.events': 3, 'txn.events.dlq': 1}
RETENTION_MS = 7 * 24 * 3600 * 1000
BUCKETS = ['bronze', 'silver', 'quarantine']

COLUMNS = {
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
    'wallets': ['wallet_id', 'customer_id', 'balance_kobo', 'currency', 'updated_at'],
    'transactions': [
        'txn_id',
        'customer_id',
        'merchant_id',
        'agent_id',
        'channel',
        'amount_kobo',
        'status',
        'created_at',
    ],
}


def log(msg):
    print('[owopay] %s' % msg, flush=True)


def copy_rows(conn, table, rows, target=None):
    cols = COLUMNS[table]
    with conn.cursor().copy('copy %s (%s) from stdin' % (target or table, ', '.join(cols))) as copy:
        for row in rows:
            copy.write_row([row[c] for c in cols])


def load_reference(conn, seed):
    conn.execute((Path(__file__).with_name('schema.sql')).read_text())
    customers = ref.customers(seed)
    tables = {
        'customers': customers,
        'merchants': ref.merchants(seed),
        'agents': ref.agents(seed),
        'wallets': ref.wallets(seed, customers),
    }
    for table, rows in tables.items():
        if conn.execute('select count(*) from %s' % table).fetchone()[0] >= len(rows):
            continue
        # Insert only what is missing, so customer changes from earlier days survive a reseed.
        conn.execute('create temp table _load (like %s) on commit drop' % table)
        copy_rows(conn, table, rows, target='_load')
        conn.execute('insert into %s select * from _load on conflict do nothing' % table)
        conn.execute('drop table _load')
        log('reference %-10s %s rows' % (table, format(len(rows), ',')))
    return tables


def apply_customer_changes(conn, changes):
    # The guard on updated_at makes a rerun of the same day a no-op.
    with conn.cursor() as cur:
        cur.executemany(
            """update customers
                  set kyc_tier = case when %(kind)s = 'upgrade_tier' then least(kyc_tier + 1, 3) else kyc_tier end,
                      state = case when %(kind)s = 'move_state' then %(state)s else state end,
                      updated_at = %(changed_at)s
                where customer_id = %(customer_id)s and updated_at < %(changed_at)s""",
            changes,
        )
    log('customers changed today: %d (KYC upgrades and moves)' % len(changes))


def load_day(conn, day, txns):
    start, end = gen.day_bounds(day)
    deleted = conn.execute('delete from transactions where created_at >= %s and created_at < %s', (start, end)).rowcount
    copy_rows(conn, 'transactions', txns)
    log(
        'transactions %s: %s rows%s'
        % (day, format(len(txns), ','), ' (replaced %s)' % format(deleted, ',') if deleted else '')
    )


def ensure_topics():
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({'bootstrap.servers': SETTINGS['KAFKA_BOOTSTRAP']})
    existing = admin.list_topics(timeout=10).topics
    new = [
        NewTopic(name, num_partitions=n, replication_factor=1, config={'retention.ms': str(RETENTION_MS)})
        for name, n in TOPICS.items()
        if name not in existing
    ]
    if not new:
        return
    for name, future in admin.create_topics(new).items():
        future.result()
        log('topic created: %s' % name)


def publish(topic, events, key_field):
    from confluent_kafka import Producer

    producer = Producer(
        {
            'bootstrap.servers': SETTINGS['KAFKA_BOOTSTRAP'],
            'linger.ms': 50,
            'compression.type': 'lz4',
            'queue.buffering.max.messages': 200_000,
        }
    )
    t0 = time.perf_counter()
    for event in events:
        value = json.dumps(event, ensure_ascii=False)
        key = str(event.get(key_field) or '')
        while True:
            try:
                producer.produce(topic, key=key, value=value)
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)
    remaining = producer.flush(60)
    if remaining:
        raise RuntimeError('%d events to %s were not delivered' % (remaining, topic))
    log('%-14s %s events in %.0fs' % (topic, format(len(events), ','), time.perf_counter() - t0))


def ensure_buckets():
    s3 = s3_client()
    existing = {b['Name'] for b in s3.list_buckets().get('Buckets', [])}
    for bucket in BUCKETS:
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)
            log('bucket created: %s' % bucket)


def parse_args(argv):
    p = argparse.ArgumentParser(prog='python -m owopay.generate', description=__doc__.splitlines()[0])
    p.add_argument('--day', required=True, type=dt.date.fromisoformat, help='Lagos business day, YYYY-MM-DD')
    p.add_argument('--rows', type=int, default=200_000, help='transactions for the day (default 200000)')
    p.add_argument('--seed', type=int, default=int(SETTINGS['OWOPAY_SEED']))
    p.add_argument('--chaos', default='', help='comma list from: %s (Week 7 onwards)' % ', '.join(gen.CHAOS_KINDS))
    p.add_argument('--no-events', action='store_true', help='skip Redpanda: Postgres only')
    p.add_argument('--no-buckets', action='store_true', help='skip SeaweedFS')
    args = p.parse_args(argv)
    args.chaos = tuple(c for c in args.chaos.replace(' ', '').split(',') if c)
    unknown = set(args.chaos) - set(gen.CHAOS_KINDS)
    if unknown:
        p.error('unknown chaos kind(s): %s' % ', '.join(sorted(unknown)))
    return args


def main(argv=None):
    import psycopg

    args = parse_args(argv)
    log(
        'seed %d, day %s, %s rows%s'
        % (args.seed, args.day, format(args.rows, ','), ', chaos: %s' % ','.join(args.chaos) if args.chaos else '')
    )
    with psycopg.connect(**pg_conninfo()) as conn:
        tables = load_reference(conn, args.seed)
        txns = gen.transactions(
            args.seed, args.day, args.rows, tables['customers'], tables['merchants'], tables['agents']
        )
        apply_customer_changes(conn, gen.customer_changes(args.seed, args.day, tables['customers']))
        load_day(conn, args.day, txns)
    if not args.no_events:
        ensure_topics()
        publish('txn.events', gen.txn_events(args.seed, args.day, txns, args.chaos), 'txn_id')
        publish('app.events', gen.app_events(args.seed, args.day, args.rows, tables['customers']), 'customer_id')
    if not args.no_buckets:
        ensure_buckets()
    log('done')


if __name__ == '__main__':
    sys.exit(main())
