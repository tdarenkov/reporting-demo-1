/* HL SKUs (typed). */
select
    sku,
    item_type,
    item_name,
    product_line,
    vendor_id,
    updated_at
from {{ source('bronze_hl', 'skus_import') }}
