/* AR invoice line detail. */
select
    invoice_no,
    header_seq_no,
    detail_seq_no,
    sales_acct_key,
    sku,
    amount::numeric(14, 2)   as amount,
    quantity::numeric(14, 2) as quantity,
    updated_at
from {{ source('bronze_hl', 'ar_details_import') }}
