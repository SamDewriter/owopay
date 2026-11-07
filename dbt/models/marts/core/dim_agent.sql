-- dim_agent: one row per agent banking point, current values only (Type 1).
-- App, USSD, POS and web transactions have no agent, so their agent_key in the fact is empty.

select
    {{ dbt_utils.generate_surrogate_key(['agent_id']) }} as agent_key,
    agent_id,
    business_name,
    state,
    created_at
from {{ ref('stg_agents') }}
