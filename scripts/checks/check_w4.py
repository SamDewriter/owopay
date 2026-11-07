"""make check-w4: the Week 4 pre-work checkpoints. The notebook runs the same checks, stage by stage."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import REPO_ROOT, SETTINGS, Checks, compose, pg, running_services, s3_client, topic_sizes  # noqa: E402

BRIDGE_PRODUCERS_IGNORED = {'owopay-core'}


def stage1(c):
    c.stage('Stage 1 · core stack is up')

    def postgres():
        with pg() as conn:
            return conn.execute('select version()').fetchone()[0].split(',')[0]

    def redpanda():
        sizes = topic_sizes()
        return '%d topic(s)' % len(sizes)

    def seaweedfs():
        return 'buckets: %s' % [b['Name'] for b in s3_client().list_buckets().get('Buckets', [])]

    c.run('Postgres', postgres, 'make up PROFILE=core, then make ps')
    c.run('Redpanda', redpanda, 'port 9092 must be free and published; make logs SERVICE=redpanda')
    c.run('SeaweedFS S3', seaweedfs, 'port 8333; keys in .env must match seaweedfs/s3.json')


def stage2(c):
    c.stage('Stage 2 · OwoPay seeded')

    def tables():
        with pg() as conn:
            counts = {
                t: conn.execute('select count(*) from %s' % t).fetchone()[0]
                for t in ['customers', 'merchants', 'agents', 'wallets', 'transactions']
            }
        if not counts['transactions']:
            raise RuntimeError('no transactions')
        return ', '.join('%s %s' % (t, format(n, ',')) for t, n in counts.items())

    def topics():
        sizes = topic_sizes()
        missing = [t for t in ('txn.events', 'app.events') if not sizes.get(t)]
        if missing:
            raise RuntimeError('empty or missing: %s' % missing)
        return ', '.join('%s %s' % (t, format(n, ',')) for t, n in sorted(sizes.items()))

    def buckets():
        names = {b['Name'] for b in s3_client().list_buckets().get('Buckets', [])}
        missing = {'bronze', 'silver', 'quarantine'} - names
        if missing:
            raise RuntimeError('missing buckets: %s' % sorted(missing))
        return 'bronze, silver, quarantine'

    c.run('Postgres tables', tables, 'make seed DAY=2026-11-03 ROWS=200000')
    c.run('Topics', topics, 'make seed publishes txn.events and app.events')
    c.run('Buckets', buckets, 'make seed creates the buckets')


def stage3(c):
    c.stage('Stage 3 · bridge lab: your DAG and your producer')

    def dag():
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
        own = sorted(d['dag_id'] for d in dags if d['dag_id'] != 'owopay_smoke')
        if not own:
            raise RuntimeError('only owopay_smoke is listed; copy your DAG into airflow/dags/')
        return 'your DAG(s): %s' % own

    def producer():
        from confluent_kafka import Consumer, TopicPartition

        consumer = Consumer(
            {
                'bootstrap.servers': SETTINGS['KAFKA_BOOTSTRAP'],
                'group.id': 'owopay-check-w4',
                'enable.auto.commit': False,
            }
        )
        try:
            meta = consumer.list_topics('txn.events', timeout=5).topics['txn.events']
            parts = []
            for p in meta.partitions:
                lo, hi = consumer.get_watermark_offsets(TopicPartition('txn.events', p), timeout=5)
                parts.append(TopicPartition('txn.events', p, max(lo, hi - 5000)))
            consumer.assign(parts)
            producers, idle = set(), 0
            while idle < 3:
                batch = consumer.consume(1000, timeout=1.0)
                idle = idle + 1 if not batch else 0
                for m in batch:
                    if m.error() is None:
                        try:
                            producers.add(json.loads(m.value()).get('producer', 'unknown'))
                        except ValueError:
                            producers.add('not JSON')
        finally:
            consumer.close()
        own = sorted(producers - BRIDGE_PRODUCERS_IGNORED)
        if not own:
            raise RuntimeError('no events from your producer in the latest 5,000 per partition')
        return 'producer(s) seen: %s' % own

    c.run('Airflow DAG', dag, 'make up PROFILE=orchestration; copy your DAG into airflow/dags/')
    c.run('Bridge producer', producer, 'run ingestion/bridge/producer.py (or the reference producer) after seeding')


def stage45(c):
    c.stage('Stages 4 and 5 · design files')
    files = {
        'Capacity model': ('docs/design/capacity_model.md', 'Assumptions'),
        'System design v0': ('docs/design/system-design-v0.md', 'four questions'),
        'ADR-001': ('docs/adr/ADR-001-storage-topology.md', 'Decision'),
    }
    for name, (path, must_contain) in files.items():

        def exists(path=path, must_contain=must_contain):
            if not (REPO_ROOT / path).exists():
                raise RuntimeError('%s is not written yet' % path)
            text = (REPO_ROOT / path).read_text()
            if must_contain not in text:
                raise RuntimeError('%s has no "%s" section' % (path, must_contain))
            return path

        c.run(name, exists, 'run the Week 4 notebook, sections 4 and 5')


if __name__ == '__main__':
    checks = Checks('make check-w4')
    stage1(checks)
    stage2(checks)
    stage3(checks)
    stage45(checks)
    checks.finish()
