select date, org, id, line_number,
       sku, sku_code, sku_name, contents,
       amount::numeric(14, 2) as amount,
       quantity::numeric(14, 2) as quantity, updated_at
from {{ source('bronze_hlm', 'costs_import') }}
