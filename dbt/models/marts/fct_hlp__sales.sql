select
    s.date,
    'HLP'::text                       as subsidiary,
    s.sku                             as sku_local,
    s.customer_id                     as customer_id_local,
    s.detail_amount                   as fx_amount,
    s.detail_amount                   as usd_amount,
    s.quantity,
    s.invoice_id || '-' || s.detail_id as transaction_id,
    s.updated_at
from {{ ref('stg_hlp__sales') }} s
