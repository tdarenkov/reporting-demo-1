select date, invoice_id, doc_num, customer_id,
       header_amount::numeric(14, 2) as header_amount,
       detail_id, sku,
       detail_amount::numeric(14, 2) as detail_amount,
       quantity::numeric(14, 2) as quantity, updated_at
from {{ source('bronze_hlp', 'sales_import') }}
