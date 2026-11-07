-- dim_date: one row per calendar day. Generated, not extracted: a calendar has no source system.
-- Days are Lagos business days, the same days the extract and the fact use.

with days as (
    select d::date as date_day
    from generate_series('2025-01-01'::date, '2027-12-31'::date, interval '1 day') as d
)

select
    to_char(date_day, 'YYYYMMDD')::int          as date_key,          -- 20261103
    date_day,
    trim(to_char(date_day, 'Day'))              as day_name,
    extract(isodow from date_day)::int          as iso_day_of_week,   -- Monday 1 … Sunday 7
    extract(isodow from date_day) in (6, 7)     as is_weekend,
    extract(day from date_day) >= 25            as is_salary_window,  -- most salaries land from the 25th
    date_trunc('month', date_day)::date         as month_start,
    extract(year from date_day)::int            as year
from days
