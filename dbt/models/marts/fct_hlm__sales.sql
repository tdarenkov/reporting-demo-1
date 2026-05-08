select
    s.date,
    'HLM'::text                  as subsidiary,
    s.sku                        as sku_local,
    s.customer_name              as customer_id_local,
    s.amount::numeric(14, 2)     as fx_amount,
    s.amount::numeric(14, 2)     as usd_amount,
    s.quantity,
    s.id || '-' || s.line_number as transaction_id,
    s.updated_at
from {{ ref('stg_hlm__sales') }} s
