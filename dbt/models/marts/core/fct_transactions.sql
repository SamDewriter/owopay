-- fct_transactions
-- Grain: one row per transaction, in its latest status (ADR-002).
-- Keys point at the dimensions; channel and status live here because they have no attributes of
-- their own ("degenerate dimensions"). The only measure is amount_kobo: add it up, never average it blind.
--
-- Left joins on purpose: a transaction whose customer or merchant is missing still lands here with an
-- empty key, and a test catches it. An inner join would drop it silently and finance would never know.
--
-- The post-hook indexes date_key because nearly every question starts with a date. The index has no
-- name: dbt builds the new table beside the old one and swaps them, and a fixed name would collide.

{{ config(post_hook="create index on {{ this }} (date_key)") }}

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

customers as (
    select * from {{ ref('dim_customer') }}
),

merchants as (
    select * from {{ ref('dim_merchant') }}
),

agents as (
    select * from {{ ref('dim_agent') }}
)

select
    t.txn_id,
    to_char(t.wat_day, 'YYYYMMDD')::int     as date_key,
    c.customer_key,
    m.merchant_key,
    a.agent_key,
    t.channel,
    t.status,
    t.amount_kobo,
    t.created_at,
    t.wat_day
from transactions t
left join customers c
    on  c.customer_id = t.customer_id
    and t.created_at >= c.valid_from          -- the customer as they were when the money moved
    and t.created_at <  c.valid_to
left join merchants m on m.merchant_id = t.merchant_id
left join agents a    on a.agent_id = t.agent_id
