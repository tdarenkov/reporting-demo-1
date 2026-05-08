/*
    HLS sales: GL revenue postings (account 90.01.1) joined to the
    partner + SKU catalogs via the dim_*_key columns.
*/
with revenue as (
    select gl.*
    from {{ ref('stg_hls__gl') }} gl
    inner join {{ ref('stg_hls__accounts') }} a on a.account_key = gl.accountcr_key
    where a.account = '90.01.1'
)
select
    r.date,
    r.id,
    r.line_number,
    partners.dim_description as customer_name,
    skus.sku as sku_local,
    r.amount,
    r.quantity_credit as quantity,
    r.updated_at
from      revenue r
left join {{ ref('stg_hls__partners') }} partners on partners.dim_key = r.dim_dr1
left join {{ ref('stg_hls__skus') }}     skus     on skus.dim_key = r.dim_cr1
