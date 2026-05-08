/*
    Customers — filtered to those referenced in gl_import, with a small
    country-cleanup pass: infer the parent country from the state code
    when country is null.
*/
with customers_used as (
    select distinct customer_id
    from {{ ref('stg_hlb__gl') }}
    where customer_id is not null
),
us_states as (
    select unnest(array[
        'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY',
        'LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND',
        'OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'
    ]) as state_abbr
)
select
    c.customer_id,
    c.customer_name,
    case
        when c.customer_country is null and s.state_abbr is not null then 'United States'
        when lower(c.customer_country) like '%china%'                 then 'China'
        when lower(c.customer_country) like '%taiwan%'                then 'Taiwan'
        when c.customer_country is null                                then 'No Country'
        else c.customer_country
    end as customer_country,
    c.customer_zip,
    c.customer_state,
    c.updated_at
from {{ source('bronze_hlb', 'customers_import') }} c
inner join customers_used u on c.customer_id = u.customer_id
left  join us_states         s on c.customer_state = s.state_abbr
