"""Helpers the demo, checks and notebooks share: PASS/FAIL lines and the three clients."""

import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'generator'))

from owopay.config import REPO_ROOT, SETTINGS, pg_conninfo, s3_client  # noqa: E402

__all__ = ['REPO_ROOT', 'SETTINGS', 'pg_conninfo', 's3_client', 'Checks', 'pg', 'topic_sizes', 'compose']


class Checks:
    """Collects PASS / FAIL lines and exits non-zero if any failed."""

    def __init__(self, title):
        self.title, self.failed = title, 0
        print(title)

    def stage(self, name):
        print('\n%s' % name)

    def run(self, name, fn, hint):
        try:
            print('  PASS  %s: %s' % (name, fn()))
        except Exception as e:  # noqa: BLE001 - every failure becomes a readable line
            self.failed += 1
            print('  FAIL  %s: %s' % (name, e))
            print('        hint: %s' % hint)

    def finish(self):
        print('\n%s: %s' % (self.title, 'all PASS' if not self.failed else '%d FAIL' % self.failed))
        sys.exit(1 if self.failed else 0)


def pg(db=None):
    import psycopg

    return psycopg.connect(**pg_conninfo(db))


def topic_sizes(timeout=5):
    """Messages currently retained per topic (sum of partition high minus low watermarks)."""
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({'bootstrap.servers': SETTINGS['KAFKA_BOOTSTRAP'], 'group.id': 'owopay-sizes'})
    try:
        sizes = {}
        for name, meta in c.list_topics(timeout=timeout).topics.items():
            if name.startswith('_'):
                continue
            total = 0
            for p in meta.partitions:
                lo, hi = c.get_watermark_offsets(TopicPartition(name, p), timeout=timeout)
                total += hi - lo
            sizes[name] = total
        return sizes
    finally:
        c.close()


def compose(*args, check=True):
    cmd = ['docker', 'compose', '--profile', '*', *args]
    out = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    if check and out.returncode:
        raise RuntimeError((out.stderr or out.stdout).strip().splitlines()[-1])
    return out.stdout


def running_services():
    rows = compose('ps', '--format', 'json')
    lines = [json.loads(line) for line in rows.splitlines() if line.strip()]
    return {r['Service']: r.get('Health') or r.get('State') for r in lines}
