select dim_key, dim_description, sku, source_file, updated_at
from {{ source('bronze_hls', 'sku_catalog_import') }}
