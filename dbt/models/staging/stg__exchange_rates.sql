/*
    Monthly-average FX rates to USD, one gap-free row per (yearmonth, currency).

    Mirrors the original Fabric `avg_yearmonth_rates` view: daily observed rates
    (the `exchange_rates` seed -- public Open Exchange Rates data, pulled live in
    production, shipped as a seed here) are averaged per calendar month and keyed
    on `yearmonth` (YYYYMM as an int). The per-subsidiary marts join this on the
    transaction's year+month, so a sale on any day inherits its month's rate.

    The series is made complete across every calendar month (the original
    enforced the same guarantee): a month with no observation carries the last
    known rate forward (weekends, holidays, or sparse months never leave a sale
    without a rate), and months before a currency's first observation take its
    earliest rate. So the marts can join plainly and `usd_amount` is never NULL.
    `rate_to_usd` converts local to USD: `usd = local * rate_to_usd`. USD subs
    (HL, HLB) skip the join (rate = 1).
*/
with observed as (
    select
        extract(year from date)::int * 100 + extract(month from date)::int as yearmonth,
        currency,
        avg(rate_to_usd)::numeric(18, 8) as rate_to_usd
    from {{ ref('exchange_rates') }}
    group by 1, currency
),

-- each currency's earliest observed rate, used to back-fill leading months
first_rate as (
    select distinct on (currency) currency, rate_to_usd
    from observed
    order by currency, yearmonth
),

-- every (calendar month x currency) combination that could need a rate
spine as (
    select cal.yearmonth, cur.currency
    from (select distinct yearmonth from {{ ref('stg_calendar') }}) cal
    cross join (select distinct currency from observed) cur
),

joined as (
    select
        s.yearmonth,
        s.currency,
        o.rate_to_usd,
        -- running count of observations seen so far per currency: rows after an
        -- observation share its group until the next observation appears
        count(o.rate_to_usd) over (
            partition by s.currency order by s.yearmonth
        ) as grp
    from spine s
    left join observed o
           on o.yearmonth = s.yearmonth and o.currency = s.currency
),

filled as (
    select
        yearmonth,
        currency,
        -- forward-fill: carry the observation that opened this group
        first_value(rate_to_usd) over (
            partition by currency, grp order by yearmonth
        ) as rate_to_usd
    from joined
)

select
    f.yearmonth,
    f.currency,
    -- leading months (before a currency's first observation, grp = 0) have no
    -- rate to carry forward, so fall back to its earliest observed rate
    coalesce(f.rate_to_usd, fr.rate_to_usd)::numeric(18, 8) as rate_to_usd
from filled f
join first_rate fr on fr.currency = f.currency
