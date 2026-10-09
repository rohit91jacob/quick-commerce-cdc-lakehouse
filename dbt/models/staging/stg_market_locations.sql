-- REAL: shops (OpenStreetMap places) where prices were observed.
select location_id, osm_name as shop_name, brand as shop_brand, city, country_code, lat, lon
from {{ source('silver', 'market_locations') }}
where not _is_deleted
