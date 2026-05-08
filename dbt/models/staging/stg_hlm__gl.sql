select
    date,
    org,
    id,
    line_number,
    account_dr,
    dim_dr1_type, dim_dr1_value,
    dim_dr2_type, dim_dr2_value,
    account_cr,
    dim_cr1_type, dim_cr1_value,
    dim_cr2_type, dim_cr2_value,
    contents,
    amount::numeric(14, 2) as amount,
    quantity_credit::numeric(14, 2) as quantity_credit,
    updated_at
from {{ source('bronze_hlm', 'gl_import') }}
