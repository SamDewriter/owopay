-- stg_transactions: one row per transaction, exactly as the source has it, renamed and typed.
-- Staging never joins and never filters. If a number is wrong later, this is where we start looking.

select
    txn_id,
    customer_id,
    merchant_id,
    agent_id,
    lower(channel)              as channel,
    lower(status)               as status,
    amount_kobo::bigint         as amount_kobo,   -- money stays whole kobo until the very last select
    created_at,                                   -- timestamptz, stored in UTC
    _wat_day                    as wat_day,       -- the Lagos business day the extract assigned
    _extracted_at               as extracted_at
from {{ source('raw', 'transactions') }}
