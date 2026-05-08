select sku_id, sku, item_name, item_type, updated_at
from {{ source('bronze_hlp', 'skus_import') }}
