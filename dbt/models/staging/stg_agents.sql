-- stg_agents: one row per agent banking point (cash in, cash out).

select
    agent_id,
    business_name,
    state,
    created_at
from {{ source('raw', 'agents') }}
