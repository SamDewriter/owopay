-- dim_merchant: one row per merchant, current values only (Type 1: a change overwrites).
-- The surrogate key is the warehouse's own ID, so the fact never depends on the source's ID format.

select
    {{ dbt_utils.generate_surrogate_key(['merchant_id']) }} as merchant_key,
    merchant_id,
    business_name,
    category,
    state,
    created_at
from {{ ref('stg_merchants') }}
