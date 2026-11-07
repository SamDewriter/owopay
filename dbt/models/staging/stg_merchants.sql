-- stg_merchants: one row per merchant.

select
    merchant_id,
    business_name,
    category,
    state,
    created_at
from {{ source('raw', 'merchants') }}
