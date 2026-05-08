select
    s.date,
    'HLD'::text                  as subsidiary,
    s.sku                        as sku_local,
    s.customer_number            as customer_id_local,
    s.amount::numeric(14, 2)     as fx_amount,
    s.amount::numeric(14, 2)     as usd_amount,
    s.quantity,
    s.doc_id || '-' || s.detail_id as transaction_id,
    s.updated_at
from {{ ref('stg_hld__sales') }} s
