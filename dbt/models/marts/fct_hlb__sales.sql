/*
    HLB sales fact — one row per (transaction_id, line_order) on a
    revenue account. Kept at the same column shape as the other
    fct_<sub>__sales models so they UNION cleanly into fct_sales_all.
*/
select
    date,
    'HLB'::text                                  as subsidiary,
    sku                                          as sku_local,
    customer_id                                  as customer_id_local,
    (-amount)::numeric(14, 2)                    as fx_amount,
    (-amount)::numeric(14, 2)                    as usd_amount,
    quantity,
    transaction_id::text || '-' || line_order::text as transaction_id,
    updated_at
from {{ ref('int_hlb__gl_enriched') }}
where account_type = 'Income'
  and account != '4010'   -- exclude the discount account (see marts.yml: fct_hlb)
