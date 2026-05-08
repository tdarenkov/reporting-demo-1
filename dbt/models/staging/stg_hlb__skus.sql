/*
    SKUs — filtered to those referenced on revenue accounts (4xxx, except
    the discount account 4010). Coalesces description/name so downstream
    consumers always have a non-null label.
*/
with skus_on_revenue as (
    select distinct sku
    from {{ ref('stg_hlb__gl') }}
    where sku is not null
      and account like '4%'
      and account != '4010'
)
select
    s.sku,
    s.sku_type,
    coalesce(s.sku_description, s.sku_name, 'No Description') as sku_name,
    s.updated_at
from {{ source('bronze_hlb', 'skus_import') }} s
inner join skus_on_revenue u on s.sku = u.sku
