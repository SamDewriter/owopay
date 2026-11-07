-- stg_customers: one row per customer, current values.
-- Personal data stops here: full_name, phone_number and bvn are in raw.customers and nowhere after.
-- Nothing downstream needs them, so nothing downstream gets them (Nigeria Data Protection Act).

select
    customer_id,
    state,
    kyc_tier,
    signup_channel,
    created_at,
    updated_at        -- when kyc_tier or state last changed; the snapshot uses it as the version time
from {{ source('raw', 'customers') }}
