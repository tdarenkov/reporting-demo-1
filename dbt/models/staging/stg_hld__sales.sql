/* HLD sales — already at line grain in source. */
select
    doc_id,
    doc_num,
    process_id,
    period,
    customer_number,
    date,
    amount_header::numeric(14, 2) as amount_header,
    detail_id,
    process_pos_id,
    salesperson_id,
    sku,
    amount::numeric(14, 2)         as amount,
    quantity::numeric(14, 2)       as quantity,
    updated_at
from {{ source('bronze_hld', 'sales_import') }}
