{#
  reset_warehouse: drop everything dbt built in dw (staging, snapshots, marts), so the next
  `dbt build` starts from nothing. raw is never touched: it belongs to the extract, not to dbt.

  A macro run on its own with run-operation, not inside a model. Dry run by default:
      make dbt ARGS="run-operation reset_warehouse"                          # prints what it would drop
      make dbt ARGS="run-operation reset_warehouse --args '{dry_run: false}'"   # drops it

  Dropping snapshots deletes customer history for good. Fine on a course laptop; never in production.
#}
{% macro reset_warehouse(dry_run=true) %}
    {% for schema in ['staging', 'snapshots', 'marts'] %}
        {% set statement %}drop schema if exists {{ schema }} cascade{% endset %}
        {% if dry_run %}
            {{ log('dry run, would execute: ' ~ statement, info=true) }}
        {% else %}
            {% do run_query(statement) %}
            {{ log('executed: ' ~ statement, info=true) }}
        {% endif %}
    {% endfor %}
{% endmacro %}
