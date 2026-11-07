"""One Lagos business day of OwoPay activity, built in memory with no I/O.

Pure functions of (seed, day, rows), so tests can check determinism without a database.
"""

import datetime as dt
import math
import random
import uuid

from . import reference as ref

WAT = ref.WAT

CHANNELS = {'app': 45, 'ussd': 25, 'pos': 15, 'agent': 10, 'web': 5}
# Share of each channel's transactions that pay a merchant; the rest are transfers, or cash for agents.
MERCHANT_SHARE = {'app': 0.40, 'ussd': 0.20, 'pos': 1.0, 'agent': 0.0, 'web': 0.60}
# Typical amount in naira and its spread, per channel (log-normal).
AMOUNT_NAIRA = {
    'app': (8_000, 1.1),
    'ussd': (3_000, 0.9),
    'pos': (6_000, 1.0),
    'agent': (15_000, 0.9),
    'web': (12_000, 1.1),
}
# Largest single transaction per KYC tier, in naira (shaped on CBN tiered KYC limits).
TIER_LIMIT_NAIRA = {1: 50_000, 2: 200_000, 3: 5_000_000}
# Activity by hour of the Lagos day: quiet overnight, busiest in the evening.
HOUR_WEIGHTS = [
    0.5,
    0.3,
    0.2,
    0.2,
    0.3,
    0.8,
    1.5,
    2.5,
    3.5,
    4.5,
    5.0,
    5.0,
    5.5,
    5.5,
    5.0,
    5.0,
    5.5,
    6.0,
    6.5,
    6.0,
    5.0,
    4.0,
    2.5,
    1.2,
]
APP_EVENTS = {'app_open': 35, 'login': 20, 'view_balance': 25, 'start_transfer': 12, 'logout': 8}
PLATFORMS = {'android': 78, 'ios': 15, 'web': 7}


def day_bounds(day):
    """The Lagos business day as a half-open UTC-aware range."""
    start = dt.datetime.combine(day, dt.time(0), tzinfo=WAT)
    return start, start + dt.timedelta(days=1)


def _rng(seed, day, stream):
    return random.Random('%s|%s|%s' % (seed, day.isoformat(), stream))


def _uuid(rng):
    return uuid.UUID(int=rng.getrandbits(128), version=4)


def _weighted(rng, table):
    return rng.choices(list(table), weights=list(table.values()))[0]


def _timestamp(rng, start):
    hour = rng.choices(range(24), weights=HOUR_WEIGHTS)[0]
    return start + dt.timedelta(hours=hour, seconds=rng.randrange(3600), microseconds=rng.randrange(1_000_000))


def _amount_kobo(rng, channel, tier):
    typical, spread = AMOUNT_NAIRA[channel]
    naira = min(int(rng.lognormvariate(math.log(typical), spread)), TIER_LIMIT_NAIRA[tier])
    kobo = max(naira, 50) * 100
    if rng.random() < 0.15:
        kobo += rng.randrange(1, 100)
    return kobo


def _status(rng, created_at, end):
    # Transfers started in the last ten minutes of the day are often still in flight at midnight.
    if end - created_at < dt.timedelta(minutes=10) and rng.random() < 0.3:
        return 'pending'
    r = rng.random()
    return 'settled' if r < 0.935 else 'failed' if r < 0.995 else 'pending'


def transactions(seed, day, rows, customers, merchants, agents):
    rng = _rng(seed, day, 'transactions')
    start, end = day_bounds(day)
    out = []
    for _ in range(rows):
        # A few customers do most of the transacting.
        customer = customers[int(len(customers) * rng.random() ** 1.6)]
        channel = _weighted(rng, CHANNELS)
        merchant = rng.choice(merchants)['merchant_id'] if rng.random() < MERCHANT_SHARE[channel] else None
        agent = rng.choice(agents)['agent_id'] if channel == 'agent' else None
        created_at = _timestamp(rng, start)
        out.append(
            {
                'txn_id': _uuid(rng),
                'customer_id': customer['customer_id'],
                'merchant_id': merchant,
                'agent_id': agent,
                'channel': channel,
                'amount_kobo': _amount_kobo(rng, channel, customer['kyc_tier']),
                'status': _status(rng, created_at, end),
                'created_at': created_at,
            }
        )
    out.sort(key=lambda t: t['created_at'])
    return out


def customer_changes(seed, day, customers):
    """Customers who upgrade KYC tier or move state on this day: the source of Week 5's history."""
    rng = _rng(seed, day, 'customer_changes')
    start, _ = day_bounds(day)
    changes = []
    for customer in rng.sample(customers, k=max(1, len(customers) // 250)):
        changes.append(
            {
                'customer_id': customer['customer_id'],
                'kind': 'move_state' if rng.random() < 0.2 else 'upgrade_tier',
                'state': _weighted(rng, ref.STATES),
                'changed_at': _timestamp(rng, start),
            }
        )
    return changes


def txn_events(seed, day, txns, chaos=()):
    """The status events each transaction emits on txn.events: initiated, authorised, then settled or failed."""
    rng = _rng(seed, day, 'txn_events')
    events = []
    for t in txns:
        steps = [('initiated', 0.0)]
        if t['status'] == 'failed' and rng.random() < 0.4:
            steps.append(('failed', rng.uniform(0.2, 3)))
        else:
            authorised = rng.uniform(0.3, 3)
            steps.append(('authorised', authorised))
            if t['status'] != 'pending':
                steps.append((t['status'], authorised + rng.uniform(1, 30)))
        for event_type, delay in steps:
            events.append(
                {
                    'event_id': str(_uuid(rng)),
                    'txn_id': str(t['txn_id']),
                    'event_type': event_type,
                    'customer_id': t['customer_id'],
                    'merchant_id': t['merchant_id'],
                    'agent_id': t['agent_id'],
                    'channel': t['channel'],
                    'amount_kobo': t['amount_kobo'],
                    'occurred_at': (t['created_at'] + dt.timedelta(seconds=delay)).isoformat(),
                    'producer': 'owopay-core',
                    'schema_version': 1,
                }
            )
    return apply_chaos(rng, events, chaos)


def app_events(seed, day, rows, customers):
    rng = _rng(seed, day, 'app_events')
    start, _ = day_bounds(day)
    events = []
    for _ in range(rows // 2):
        customer = customers[int(len(customers) * rng.random() ** 1.6)]
        events.append(
            {
                'event_id': str(_uuid(rng)),
                'customer_id': customer['customer_id'],
                'event_type': _weighted(rng, APP_EVENTS),
                'platform': _weighted(rng, PLATFORMS),
                'app_version': '5.%d.%d' % (rng.randrange(3, 8), rng.randrange(10)),
                'occurred_at': _timestamp(rng, start).isoformat(),
                'producer': 'owopay-app',
                'schema_version': 1,
            }
        )
    events.sort(key=lambda e: e['occurred_at'])
    return events


CHAOS_KINDS = ('duplicates', 'late', 'bad_records')


def apply_chaos(rng, events, chaos):
    """Week 7 onwards: the faults a real event stream has. Off unless asked for."""
    if not chaos:
        return events
    out = list(events)
    if 'duplicates' in chaos:
        out += [dict(e) for e in rng.sample(events, k=len(events) // 50)]
    if 'late' in chaos:
        for e in rng.sample(out, k=len(out) // 100):
            e['occurred_at'] = (dt.datetime.fromisoformat(e['occurred_at']) - dt.timedelta(hours=26)).isoformat()
    if 'bad_records' in chaos:
        for e in rng.sample(out, k=len(out) // 200):
            fault = rng.choice(['negative_amount', 'amount_as_text', 'missing_txn_id', 'unknown_channel'])
            if fault == 'negative_amount':
                e['amount_kobo'] = -e['amount_kobo']
            elif fault == 'amount_as_text':
                e['amount_kobo'] = '₦%s' % format(e['amount_kobo'] / 100, ',.2f')
            elif fault == 'missing_txn_id':
                e.pop('txn_id')
            else:
                e['channel'] = 'carrier_pigeon'
    rng.shuffle(out)
    return out
