{#
  not_in_the_future: a generic test, which is a macro that returns the failing rows. Any column can
  use it from YAML, the same way it uses unique or not_null:

      - name: created_at
        data_tests: [not_in_the_future]

  A transaction stamped in the future means a clock is wrong somewhere upstream (a POS terminal, an
  agent's phone). Five minutes of grace allows for ordinary clock drift.
#}
{% test not_in_the_future(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} > now() + interval '5 minutes'
{% endtest %}
