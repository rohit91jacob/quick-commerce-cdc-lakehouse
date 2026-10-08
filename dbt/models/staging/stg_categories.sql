select category_id, name as category_name, is_perishable
from {{ source('silver', 'categories') }}
where not _is_deleted
