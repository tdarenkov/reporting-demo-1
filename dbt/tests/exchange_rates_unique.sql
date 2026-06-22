/*
    Singular test: at most one rate per (yearmonth, currency) in the staged
    monthly exchange-rate table. Returns offending keys; empty result = pass.
*/
select yearmonth, currency, count(*) as n
from {{ ref('stg__exchange_rates') }}
group by yearmonth, currency
having count(*) > 1
