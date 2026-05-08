select sku_id, sku, sku_name, sku_full_name, item_type, updated_at
from {{ source('bronze_hlm', 'skus_import') }}
