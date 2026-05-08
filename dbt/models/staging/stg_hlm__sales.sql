select date, org, id, line_number,
       customer_name, vat_rate_text, sku, sku_code, sku_name, sku_full_name,
       contents, amount::numeric(14, 2) as amount,
       quantity::numeric(14, 2) as quantity, updated_at
from {{ source('bronze_hlm', 'sales_import') }}
