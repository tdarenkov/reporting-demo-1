/*
    Cross-subsidiary sales fact — the report-facing UNION mart. Columns are
    listed explicitly (not `select *`) so the UNION can't silently misalign
    if a per-sub mart's column order ever drifts.
*/
select date, subsidiary, sku_local, customer_id_local, fx_amount, usd_amount, quantity, transaction_id, updated_at from {{ ref('fct_hlb__sales') }}
union all
select date, subsidiary, sku_local, customer_id_local, fx_amount, usd_amount, quantity, transaction_id, updated_at from {{ ref('fct_hl__sales') }}
union all
select date, subsidiary, sku_local, customer_id_local, fx_amount, usd_amount, quantity, transaction_id, updated_at from {{ ref('fct_hlc__sales') }}
union all
select date, subsidiary, sku_local, customer_id_local, fx_amount, usd_amount, quantity, transaction_id, updated_at from {{ ref('fct_hld__sales') }}
union all
select date, subsidiary, sku_local, customer_id_local, fx_amount, usd_amount, quantity, transaction_id, updated_at from {{ ref('fct_hlm__sales') }}
union all
select date, subsidiary, sku_local, customer_id_local, fx_amount, usd_amount, quantity, transaction_id, updated_at from {{ ref('fct_hlp__sales') }}
union all
select date, subsidiary, sku_local, customer_id_local, fx_amount, usd_amount, quantity, transaction_id, updated_at from {{ ref('fct_hls__sales') }}
