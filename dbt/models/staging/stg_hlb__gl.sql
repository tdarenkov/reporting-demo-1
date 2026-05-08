/*
    GL detail for HLB — typed + cleaned, drops the dlt internals.
    Materialized as a view (per dbt_project.yml default for staging).
*/
select
    transaction_id,
    line_order,
    date,
    account,
    customer_id,
    sku,
    amount::numeric(14, 2)   as amount,
    quantity::numeric(14, 2) as quantity,
    updated_at
from {{ source('bronze_hlb', 'gl_import') }}
