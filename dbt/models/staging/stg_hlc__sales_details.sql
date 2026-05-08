select
    parent_id,
    pos_index,
    amount::numeric(14, 2)         as amount,
    amount_no_vat::numeric(14, 2)  as amount_no_vat,
    row_type,
    quantity::numeric(14, 2)       as quantity,
    sku,
    text,
    updated_at
from {{ source('bronze_hlc', 'sales_details_import') }}
