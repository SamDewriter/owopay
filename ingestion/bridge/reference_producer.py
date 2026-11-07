"""Fallback for the Week 4 bridge lab: a few well-formed events on txn.events.

    python ingestion/bridge/reference_producer.py [--n 10]

Your own producer goes beside it as producer.py: point it at KAFKA_BOOTSTRAP and topic txn.events.
"""

import argparse
import datetime as dt
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'generator'))

from confluent_kafka import Producer  # noqa: E402

from owopay.config import SETTINGS  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--n', type=int, default=10)
    args = parser.parse_args()
    producer = Producer({'bootstrap.servers': SETTINGS['KAFKA_BOOTSTRAP']})
    now = dt.datetime.now(dt.timezone.utc)
    for i in range(args.n):
        event = {
            'event_id': str(uuid.uuid4()),
            'txn_id': str(uuid.uuid4()),
            'event_type': 'initiated',
            'customer_id': 'CUS-%06d' % (i + 1),
            'merchant_id': None,
            'agent_id': None,
            'channel': 'ussd' if i % 2 else 'app',
            'amount_kobo': 150_000 + 100 * i,
            'occurred_at': (now + dt.timedelta(seconds=i)).isoformat(),
            'producer': 'week04-reference',
            'schema_version': 1,
        }
        producer.produce('txn.events', key=event['txn_id'], value=json.dumps(event))
    producer.flush(10)
    print('%d events produced to txn.events' % args.n)


if __name__ == '__main__':
    main()
