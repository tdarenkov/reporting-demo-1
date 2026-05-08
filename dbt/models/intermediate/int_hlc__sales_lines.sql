/* HLC sales: header + detail + customer + SKU joined per detail line. */
select
    h.date,
    h.doc_num,
    d.parent_id,
    d.pos_index,
    cust.customer_id,
    cust.customer_name,
    cust.customer_country,
    d.sku,
    sku.sku_name,
    d.amount_no_vat as amount,
    d.quantity,
    h.updated_at
from      {{ ref('stg_hlc__sales_headers') }}    h
inner join {{ ref('stg_hlc__sales_details') }}   d on d.parent_id = h.id
left  join {{ ref('stg_hlc__customers') }}       cust on cust.customer_code = h.customer_code
left  join {{ ref('stg_hlc__skus') }}            sku  on sku.sku = d.sku
where d.row_type = 1     -- exclude tax / discount detail rows
