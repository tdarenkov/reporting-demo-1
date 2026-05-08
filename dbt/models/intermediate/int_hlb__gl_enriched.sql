/*
    GL detail enriched with account, customer, and SKU master data.
    Materialized as `ephemeral` (see dbt_project.yml), so dbt does NOT
    create a DB object — any downstream model that ref()s this gets
    the SELECT inlined as a CTE at compile time. Keeps the silver
    schema clean while letting the join logic compose across marts.
*/
select
    gl.transaction_id,
    gl.line_order,
    gl.date,
    gl.account,
    acct.account_type,
    acct.account_full_name,
    cust.customer_id,
    cust.customer_name,
    cust.customer_country,
    sku.sku,
    sku.sku_name,
    gl.amount,
    gl.quantity,
    gl.updated_at
from      {{ ref('stg_hlb__gl') }}        gl
left join {{ ref('stg_hlb__accounts') }}  acct on gl.account     = acct.account
left join {{ ref('stg_hlb__customers') }} cust on gl.customer_id = cust.customer_id
left join {{ ref('stg_hlb__skus') }}      sku  on gl.sku         = sku.sku
