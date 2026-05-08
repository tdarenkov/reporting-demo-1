select
    sku,
    sku_name,
    updated_at
from {{ source('bronze_hld', 'skus_import') }}
