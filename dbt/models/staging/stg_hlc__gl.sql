/* HLC GL detail. */
select
    id,
    obj_version,
    date,
    accdoc_queue_id,
    ord_num,
    account_dr,
    account_cr,
    amount::numeric(14, 2) as amount,
    firm_id,
    updated_at
from {{ source('bronze_hlc', 'gl_import') }}
