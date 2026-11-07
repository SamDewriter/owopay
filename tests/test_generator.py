"""The generator's promises: same inputs, same data; every transaction inside its Lagos day; valid values."""

import datetime as dt

import pytest

from owopay import day as gen
from owopay import reference as ref

SEED, DAY, ROWS = 20261030, dt.date(2026, 11, 3), 2_000


@pytest.fixture(scope='module')
def refs():
    return ref.customers(SEED), ref.merchants(SEED), ref.agents(SEED)


@pytest.fixture(scope='module')
def txns(refs):
    return gen.transactions(SEED, DAY, ROWS, *refs)


def test_deterministic(refs, txns):
    assert gen.transactions(SEED, DAY, ROWS, *refs) == txns
    assert gen.txn_events(SEED, DAY, txns) == gen.txn_events(SEED, DAY, txns)


def test_days_differ(refs, txns):
    other = gen.transactions(SEED, DAY + dt.timedelta(days=1), ROWS, *refs)
    assert {t['txn_id'] for t in other}.isdisjoint({t['txn_id'] for t in txns})


def test_inside_the_lagos_day(txns):
    start, end = gen.day_bounds(DAY)
    assert all(start <= t['created_at'] < end for t in txns)
    assert start.utcoffset() == dt.timedelta(hours=1)


def test_values(txns):
    assert len(txns) == ROWS
    assert {t['channel'] for t in txns} <= set(gen.CHANNELS)
    assert {t['status'] for t in txns} <= {'settled', 'failed', 'pending'}
    assert all(isinstance(t['amount_kobo'], int) and t['amount_kobo'] > 0 for t in txns)
    assert all((t['agent_id'] is not None) == (t['channel'] == 'agent') for t in txns)
    assert all(t['merchant_id'] for t in txns if t['channel'] == 'pos')


def test_events_follow_status(txns):
    events = gen.txn_events(SEED, DAY, txns)
    last = {}
    for e in events:
        last[e['txn_id']] = e['event_type']
    for t in txns:
        expected = 'authorised' if t['status'] == 'pending' else t['status']
        assert last[str(t['txn_id'])] == expected


def test_chaos_is_opt_in(txns):
    clean = gen.txn_events(SEED, DAY, txns)
    noisy = gen.txn_events(SEED, DAY, txns, chaos=('duplicates', 'bad_records'))
    assert len(noisy) > len(clean)
    assert all(isinstance(e['amount_kobo'], int) and e['amount_kobo'] > 0 for e in clean)
