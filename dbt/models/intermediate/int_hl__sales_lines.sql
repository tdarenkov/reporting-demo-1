/*
    AR header + line + customer + SKU joined to a single row per AR
    detail line. Ephemeral — inlined as a CTE wherever ref()'d.
*/
select
    h.invoice_date              as date,
    h.invoice_no,
    h.header_seq_no,
    d.detail_seq_no,
    d.sales_acct_key            as account,
    cust.customer_id,
    cust.customer_name,
    cust.customer_country,
    sku.sku,
    sku.item_name               as sku_name,
    d.amount,
    d.quantity,
    h.updated_at
from      {{ ref('stg_hl__ar_headers') }}    h
inner join {{ ref('stg_hl__ar_details') }}   d on d.invoice_no = h.invoice_no and d.header_seq_no = h.header_seq_no
left  join {{ ref('stg_hl__customers') }}    cust on cust.customer_id = h.customer_id
left  join {{ ref('stg_hl__skus') }}         sku  on sku.sku = d.sku
