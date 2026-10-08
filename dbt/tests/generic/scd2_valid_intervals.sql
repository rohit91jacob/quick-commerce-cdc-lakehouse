{#
  A type-2 history is valid when every key has exactly one current version, versions do not overlap,
  and each version ends exactly where the next one starts.
#}
{% test scd2_valid_intervals(model, key_column) %}
with versions as (
    select
        {{ key_column }} as entity_key,
        version,
        valid_from,
        valid_to,
        is_current,
        lead(valid_from) over (partition by {{ key_column }} order by version) as next_valid_from
    from {{ model }}
),

problems as (
    select entity_key, 'interval does not meet the next version' as problem
    from versions
    where next_valid_from is not null and valid_to <> next_valid_from
    union all
    select entity_key, 'interval ends before it starts'
    from versions
    where valid_to < valid_from
    union all
    select entity_key, 'not exactly one current version'
    from versions
    group by entity_key
    having count_if(is_current) <> 1
)

select * from problems
{% endtest %}
