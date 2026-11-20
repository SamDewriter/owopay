{#
  kobo_to_naira: money is stored as whole kobo (bigint) everywhere; naira appears only in the very last
  column a person reads. Dividing by 100.0 (not 100) keeps it exact numeric: 12345 kobo -> 123.45.

  Used as:  {{ kobo_to_naira('sum(amount_kobo)') }} as amount_naira
#}
{% macro kobo_to_naira(kobo) -%}
    ({{ kobo }}) / 100.0
{%- endmacro %}
