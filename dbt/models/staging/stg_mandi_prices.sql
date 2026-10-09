-- REAL (optional): Agmarknet daily wholesale prices, rupees per quintal.
select state, district, market, commodity, variety, grade, arrival_date, min_price, max_price, modal_price
from {{ source('silver', 'mandi_prices') }}
where not _is_deleted
