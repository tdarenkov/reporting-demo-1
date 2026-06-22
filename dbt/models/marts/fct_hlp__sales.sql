/* HLP sales fact — local PLN amounts translated to USD via stg__exchange_rates. */
select
    s.date,
    'HLP'::text                                                   as subsidiary,
    s.sku                                                         as sku_local,
    s.customer_id                                                 as customer_id_local,
    s.detail_amount::numeric(14, 2)                               as fx_amount,
    round(s.detail_amount * fx.rate_to_usd, 2)::numeric(14, 2)    as usd_amount,
    s.quantity,
    s.invoice_id || '-' || s.detail_id                            as transaction_id,
    s.updated_at
from {{ ref('stg_hlp__sales') }} s
left join {{ ref('stg__exchange_rates') }} fx
       on fx.yearmonth = extract(year from s.date)::int * 100 + extract(month from s.date)::int
      and fx.currency = 'PLN'
