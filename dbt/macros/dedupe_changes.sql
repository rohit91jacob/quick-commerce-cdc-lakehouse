{#
  Bronze is idempotent per (key, LSN, op), but replays can still differ in delivery metadata.
  Keep one row per change, ordered for SCD2 / interval logic.
#}
{% macro dedupe_changes(relation, key_columns) -%}
    select *
    from (
        select
            c.*,
            row_number() over (
                partition by {{ key_columns | join(', ') }}, _lsn, _op
                order by _ingested_at
            ) as _dup_rank
        from {{ relation }} c
    )
    where _dup_rank = 1
{%- endmacro %}
