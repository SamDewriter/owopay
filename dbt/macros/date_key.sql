{#
  date_key: the integer key of dim_date, yyyymmdd (2026-11-03 -> 20261103).
  dim_date makes the key and fct_transactions looks it up. Both call this macro, so the two can
  never drift apart: change the format here and both sides change together.
#}
{% macro date_key(date_column) -%}
    to_char({{ date_column }}, 'YYYYMMDD')::int
{%- endmacro %}
