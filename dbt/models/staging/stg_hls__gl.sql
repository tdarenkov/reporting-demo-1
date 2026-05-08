select
    date,
    id,
    line_number,
    accountdr_key,
    accountcr_key,
    dim_dr1, dim_dr1_type,
    dim_cr1, dim_cr1_type,
    amount::numeric(14, 2) as amount,
    quantity_credit::numeric(14, 2) as quantity_credit,
    contents,
    updated_at
from {{ source('bronze_hls', 'gl_import') }}
