select
    sku,
    sku_name,
    vendor_code,
    updated_at
from {{ source('bronze_hlc', 'skus_import') }}
