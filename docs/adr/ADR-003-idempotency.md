# ADR-003: Idempotent batch extract

- Status: accepted, 2026-11-25
- Deciders: the platform team
- Related: ADR-000 (the course stack), `ingestion/batch/extract_to_dw.py`

## Context

The batch extract copies OwoPay's Lagos days from the OLTP database into `dw.raw`. It is scheduled
daily and rerun by hand after a failure, a late settlement, or a corrected seed. A rerun must not
double-count money, must not leave a half-written day, and must be provable after the fact. Two
schedulers or a retried Airflow task can overlap.

## Options considered

1. **Delete and reload each Lagos day, inside one transaction.** The existing shape. A rerun replaces
   the day; a failure rolls back; `--lookback` picks up late status changes.
2. **Upsert by `txn_id` (`insert ... on conflict do update`).** Avoids the delete, but the source is
   not append-only: rows can disappear, and an upsert cannot remove them. It also needs a unique key
   before it is safe.
3. **Append-only with deduplication downstream.** Cheapest to write, but raw would grow on every
   rerun and every model would need the same dedup rule. Reconciliation would move from raw to dbt.
4. **Truncate and reload the whole table.** Simple, but a failure loses history and a 900,000-row
   reload is wasteful for a one-day rerun.

## Decision

Keep option 1 and make the guarantees explicit in the database:

- `raw.transactions.txn_id` is `not null` and has a unique index; `raw.customers.customer_id`,
  `raw.merchants.merchant_id` and `raw.agents.agent_id` are unique and not null. A duplicate fails
  the `COPY` and rolls the whole run back, instead of being silently counted twice.
- Each run takes `pg_advisory_xact_lock(hashtext('owopay.extract')::bigint)` before writing. The lock
  is held until commit or rollback, so overlapping runs queue instead of interleaving.
- `raw.load_audit` records one row per dataset and partition (`transactions` per Lagos day, reference
  tables as `all`) with `run_id`, `row_count`, `kobo`, `status` and `loaded_at`. It is upserted in the
  same transaction as the load, so it cannot describe a load that did not commit.
- Reference tables remain truncate-and-reload whole: they are small, and a rerun must also remove
  deleted customers, merchants and agents.

`_extracted_at` stays as row-level lineage, but it changes on every run and is not the idempotency
signal. `raw.load_audit` is.

## Consequences

- A rerun of the same day produces the same rows and the same kobo total. The proof is a before/after
  fingerprint of raw and the marts plus the audit row's `run_id` and counts.
- A failure halfway leaves the previous load intact: the day delete, the `COPY` and the audit upsert
  share one transaction.
- Two extracts cannot write at once. The second waits; it does not corrupt or duplicate the first.
- Creating the unique indexes on an existing warehouse fails if duplicates already exist. Clean them
  first; after that the index is the guard.
- The advisory lock is scoped to one Postgres database. A future multi-warehouse or multi-writer
  setup needs a stronger coordination mechanism.
- `load_audit` keeps the latest successful load per partition, not a full run history. An append-only
  run log is a future change if audits need every attempt.

## Amendments

- None yet.
