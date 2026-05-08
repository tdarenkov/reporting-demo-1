select
    doc_num,
    obj_version,
    ord_num,
    date,
    amount::numeric(14, 2)         as amount,
    amount_no_vat::numeric(14, 2)  as amount_no_vat,
    trade_type,
    customer_code,
    id,
    accdoc_queue_id,
    accdoc_queue_id_obj_version,
    updated_at
from {{ source('bronze_hlc', 'sales_headers_import') }}
