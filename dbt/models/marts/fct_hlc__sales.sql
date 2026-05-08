/* HLC sales fact. */
select
    date,
    'HLC'::text                  as subsidiary,
    sku                          as sku_local,
    customer_id                  as customer_id_local,
    amount::numeric(14, 2)       as fx_amount,
    amount::numeric(14, 2)       as usd_amount,
    quantity,
    doc_num || '-' || pos_index  as transaction_id,
    updated_at
from {{ ref('int_hlc__sales_lines') }}
