-- dim_customer: one row per customer per version (Type 2). Built from the snapshot.
-- A transaction joins the version whose window [valid_from, valid_to) contains its created_at,
-- so a transfer made on tier 1 stays a tier-1 transfer after the customer upgrades.

with versions as (
    select
        *,
        row_number() over (partition by customer_id order by dbt_valid_from) as version_number
    from {{ ref('snap_customers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_key,
    customer_id,
    version_number,
    kyc_tier,
    state,
    signup_channel,
    created_at,
    -- The first version we know of is back-dated: history only starts when snapshots began,
    -- and without this every older transaction would find no customer at all.
    case when version_number = 1 then '1900-01-01'::timestamptz else dbt_valid_from end as valid_from,
    coalesce(dbt_valid_to, '9999-12-31'::timestamptz)                                 as valid_to,
    dbt_valid_to is null                                                               as is_current
from versions
