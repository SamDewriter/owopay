{#
  dbt's default would name the schemas analytics_staging and analytics_marts.
  This keeps them exactly as the folders say: staging, snapshots, marts.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
