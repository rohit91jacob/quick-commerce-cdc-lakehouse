{# Business reporting happens in India Standard Time. #}
{% macro local_ts(column) -%}
    at_timezone({{ column }}, '{{ var("local_timezone") }}')
{%- endmacro %}

{% macro local_date(column) -%}
    cast(at_timezone({{ column }}, '{{ var("local_timezone") }}') as date)
{%- endmacro %}

{% macro minutes_between(start, finish) -%}
    (date_diff('millisecond', {{ start }}, {{ finish }}) / 60000.0)
{%- endmacro %}
