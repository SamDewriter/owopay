-- The regulator's rule, as a test: every Lagos day in the fact reconciles to raw, to the kobo.
-- A dbt test passes when its query returns no rows. Any row here is a day where a transaction
-- or a single kobo went missing (or appeared) between raw and the fact.

with raw_days as (
    select _wat_day as wat_day, count(*) as txns, sum(amount_kobo) as amount_kobo
    from {{ source('raw', 'transactions') }}
    group by 1
),

fact_days as (
    select wat_day, count(*) as txns, sum(amount_kobo) as amount_kobo
    from {{ ref('fct_transactions') }}
    group by 1
)

select
    coalesce(r.wat_day, f.wat_day)  as wat_day,
    r.txns                          as raw_txns,
    f.txns                          as fact_txns,
    r.amount_kobo                   as raw_kobo,
    f.amount_kobo                   as fact_kobo
from raw_days r
full outer join fact_days f on f.wat_day = r.wat_day
where r.txns is distinct from f.txns
   or r.amount_kobo is distinct from f.amount_kobo
