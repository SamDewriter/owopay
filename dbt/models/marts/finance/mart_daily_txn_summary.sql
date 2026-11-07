-- mart_daily_txn_summary: finance's 09:00 question, answered in advance.
-- Grain: one row per Lagos day, channel and status. The group-by over every transaction happens
-- once, when dbt builds this table; the dashboard reads about a hundred rows instead of scanning
-- the fact. Kobo stays an integer to the end; naira is the very last column.

select
    f.wat_day,
    d.day_name,
    d.is_weekend,
    d.is_salary_window,
    f.channel,
    f.status,
    count(*)                    as txns,
    sum(f.amount_kobo)          as amount_kobo,
    sum(f.amount_kobo) / 100.0  as amount_naira     -- exact: numeric division, not a float
from {{ ref('fct_transactions') }} f
join {{ ref('dim_date') }} d on d.date_key = f.date_key
group by 1, 2, 3, 4, 5, 6
