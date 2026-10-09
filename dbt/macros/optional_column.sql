{#
  Select a column that may not exist yet (online schema change at the source), typed and null until it does.
  Lets a model reference a newly added source column before every environment has seen it.
#}
{% macro optional_column(relation, column_name, data_type) -%}
    {%- set columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
    {%- if column_name | lower in columns -%}
        cast({{ adapter.quote(column_name) }} as {{ data_type }}) as {{ column_name }}
    {%- else -%}
        cast(null as {{ data_type }}) as {{ column_name }}
    {%- endif -%}
{%- endmacro %}

{#- The same, as a bare expression (no alias), e.g. to wrap in coalesce(). -#}
{% macro optional_value(relation, column_name, data_type) -%}
    {%- set columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
    {%- if column_name | lower in columns -%}
        cast({{ adapter.quote(column_name) }} as {{ data_type }})
    {%- else -%}
        cast(null as {{ data_type }})
    {%- endif -%}
{%- endmacro %}
