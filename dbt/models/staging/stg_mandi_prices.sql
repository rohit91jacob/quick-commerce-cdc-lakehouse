-- REAL (optional): Agmarknet daily wholesale prices, rupees per quintal.
-- The silver table only exists once a data.gov.in key has loaded at least one row (CDC creates
-- tables on their first change), so without it this model is an empty, typed relation.
{%- set src = source('silver', 'mandi_prices') %}
{%- set relation = adapter.get_relation(database=src.database, schema=src.schema, identifier=src.identifier) %}
{% if relation is not none %}
select state, district, market, commodity, variety, grade, arrival_date, min_price, max_price, modal_price
from {{ src }}
where not _is_deleted
{% else %}
select
    cast(null as varchar) as state, cast(null as varchar) as district, cast(null as varchar) as market,
    cast(null as varchar) as commodity, cast(null as varchar) as variety, cast(null as varchar) as grade,
    cast(null as date) as arrival_date, cast(null as decimal(10, 2)) as min_price,
    cast(null as decimal(10, 2)) as max_price, cast(null as decimal(10, 2)) as modal_price
where false
{% endif %}
