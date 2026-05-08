select
    date,
    'HLS'::text                  as subsidiary,
    sku_local,
    customer_name                as customer_id_local,
    amount::numeric(14, 2)       as fx_amount,
    amount::numeric(14, 2)       as usd_amount,
    quantity,
    id || '-' || line_number     as transaction_id,
    updated_at
from {{ ref('int_hls__sales') }}
