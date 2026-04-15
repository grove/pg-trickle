{# DBT-1: Enable pg_trickle dog-feeding monitoring.
   Calls setup_dog_feeding() — idempotent, safe on every run.
   Add to dbt_project.yml: +post-hook: "{{ pgtrickle_enable_monitoring() }}" #}
{% macro pgtrickle_enable_monitoring() %}
    {% set sql %}
        SELECT pgtrickle.setup_dog_feeding();
    {% endset %}
    {% do run_query(sql) %}
    {{ log("pg_trickle dog-feeding monitoring enabled (5 DF stream tables active).", info=True) }}
{% endmacro %}
