/* HLD sales fact — local EUR amounts translated to USD via stg__exchange_rates. */
select
    s.date,
    'HLD'::text                                          as subsidiary,
    s.sku                                                as sku_local,
    s.customer_number                                    as customer_id_local,
    s.amount::numeric(14, 2)                             as fx_amount,
    round(s.amount * fx.rate_to_usd, 2)::numeric(14, 2)  as usd_amount,
    s.quantity,
    s.doc_id || '-' || s.detail_id                       as transaction_id,
    s.updated_at
from {{ ref('stg_hld__sales') }} s
left join {{ ref('stg__exchange_rates') }} fx
       on fx.yearmonth = extract(year from s.date)::int * 100 + extract(month from s.date)::int
      and fx.currency = 'EUR'
