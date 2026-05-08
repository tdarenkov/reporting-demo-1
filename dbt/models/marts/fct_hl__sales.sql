/* HL sales fact — narrowed to revenue accounts. */
select
    date,
    'HL'::text                   as subsidiary,
    sku                          as sku_local,
    customer_id                  as customer_id_local,
    amount::numeric(14, 2)       as fx_amount,
    amount::numeric(14, 2)       as usd_amount,    -- HL is USD-only
    quantity,
    invoice_no || '-' || header_seq_no || '-' || detail_seq_no as transaction_id,
    updated_at
from {{ ref('int_hl__sales_lines') }}
where account like '4%'
