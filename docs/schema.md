# OwoPay source schema

What the generator (`python -m owopay.generate`, wrapped by `make seed`) writes. OwoPay is fictional; the shapes, volumes and Nigerian conventions are realistic on purpose. Everything is deterministic for a given `OWOPAY_SEED`, day and row count, so every learner sees the same numbers.

## Conventions

- **Money** is `amount_kobo`, a whole number of kobo (`bigint`). ₦1 = 100 kobo. Never a float.
- **Time** is `timestamptz`, stored in UTC. The **business day is Lagos time** (WAT, UTC+1, no daylight saving): `make seed DAY=2026-11-03` writes transactions from 00:00 to 23:59:59 in Lagos, which is 23:00 on 2 November to 22:59:59 on 3 November in UTC.
- **IDs** are readable text for reference data (`CUS-000123`, `MER-00042`, `AGT-00007`, `WAL-000123`) and UUIDs for transactions and events.
- **Personal data**: `customers.full_name`, `phone_number` and `bvn` are synthetic but treated as real personal data. They stay in the OLTP database and in staging, never in a dimension or a mart.

## Postgres · database `owopay`

### customers — 50,000 rows, changes daily

| Column | Type | Notes |
| --- | --- | --- |
| customer_id | text, PK | `CUS-000001` … `CUS-050000` |
| full_name | text | personal data |
| phone_number | text | `+234…`, personal data |
| bvn | text | 11 digits, personal data |
| state | text | 18 states, Lagos about 30% |
| kyc_tier | smallint | 1, 2 or 3; caps a single transaction at ₦50k, ₦200k, ₦5m |
| signup_channel | text | `app`, `ussd`, `agent` |
| created_at | timestamptz | signup, 2024-01-01 to 2026-10-31 |
| updated_at | timestamptz | last change |

Each seeded day, 1 in 250 customers changes: most move up one KYC tier, about a fifth move state. `updated_at` records when. This is the history Week 5's snapshot captures. Reseeding the same day changes nothing further.

### merchants — 3,000 rows

| Column | Type | Notes |
| --- | --- | --- |
| merchant_id | text, PK | `MER-00001` … |
| business_name | text | |
| category | text | `groceries`, `food_and_drink`, `airtime_and_data`, `transport`, `fashion`, `pharmacy`, `electronics`, `bills_and_utilities`, `education`, `betting` |
| state | text | |
| created_at | timestamptz | |

### agents — 1,500 rows

Agent banking points (cash in and cash out).

| Column | Type | Notes |
| --- | --- | --- |
| agent_id | text, PK | `AGT-00001` … |
| business_name | text | |
| state | text | |
| created_at | timestamptz | |

### wallets — one per customer

| Column | Type | Notes |
| --- | --- | --- |
| wallet_id | text, PK | |
| customer_id | text, unique, FK | |
| balance_kobo | bigint | opening balance; not updated by transactions in the course |
| currency | char(3) | `NGN` |
| updated_at | timestamptz | |

### transactions — `ROWS` per seeded day

| Column | Type | Notes |
| --- | --- | --- |
| txn_id | uuid, PK | |
| customer_id | text, FK, not null | a few customers do most of the transacting |
| merchant_id | text, FK, nullable | set when the customer paid a merchant; always set for `pos` |
| agent_id | text, FK, nullable | set only when `channel = 'agent'` |
| channel | text | `app` 45%, `ussd` 25%, `pos` 15%, `agent` 10%, `web` 5% |
| amount_kobo | bigint, > 0 | log-normal per channel, capped by the customer's KYC tier |
| status | text | `settled` about 93.5%, `failed` about 6%, `pending` about 0.5%; more pending in the last ten minutes of the day |
| created_at | timestamptz | busiest 17:00–20:00 Lagos time, quietest 02:00–04:00 |

Indexes on `created_at` and `customer_id`. Reseeding a day deletes that Lagos day's rows and loads them again.

## Redpanda topics

All values are JSON. 7-day retention.

### txn.events — 3 partitions, key `txn_id`

Every transaction emits its status changes: `initiated`, then `authorised`, then `settled` or `failed`. Some failures skip `authorised`. A `pending` transaction stops at `authorised`. That is about three events per transaction.

```json
{"event_id": "uuid", "txn_id": "uuid", "event_type": "initiated|authorised|settled|failed",
 "customer_id": "CUS-…", "merchant_id": "MER-…|null", "agent_id": "AGT-…|null",
 "channel": "app", "amount_kobo": 724200, "occurred_at": "2026-11-03T23:59:59.186215+01:00",
 "producer": "owopay-core", "schema_version": 1}
```

`occurred_at` carries the Lagos offset. Your bridge producer writes the same shape with its own `producer` name.

### app.events — 3 partitions, key `customer_id`

`ROWS / 2` events a day: `app_open`, `login`, `view_balance`, `start_transfer`, `logout`, with `platform` (`android`, `ios`, `web`) and `app_version`.

### txn.events.dlq — 1 partition

Empty until Week 8, when the consumer sends events that break the contract here.

### Chaos (Week 7 onwards)

`make seed CHAOS=duplicates,late,bad_records` adds: about 2% duplicated events, about 1% with `occurred_at` 26 hours early, and about 0.5% bad records (negative amount, amount as text, missing `txn_id`, unknown channel).

## Postgres · database `dw` (from Week 5)

The warehouse. Nothing in it is written by hand.

| Schema | Holds | Written by |
| --- | --- | --- |
| `raw` | copies of `transactions` (with `_wat_day`), `customers`, `merchants`, `agents`, each with `_extracted_at` | `make extract` (`ingestion/batch/extract_to_dw.py`) |
| `staging` | views: renamed, typed, no personal data | dbt |
| `snapshots` | `snap_customers`, every customer version | dbt snapshot |
| `marts` | `dim_date`, `dim_customer`, `dim_merchant`, `dim_agent`, `fct_transactions`, `mart_daily_txn_summary` | dbt |

See `dbt/README.md` for the model and its grain.

## SeaweedFS buckets

`bronze`, `silver` and `quarantine` are created empty. Bronze and Silver fill from Week 6.
