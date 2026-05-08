/*
    Chart of accounts. Filters to accounts actually referenced in gl_import
    so downstream marts only see live accounts (the original Fabric
    bronze_hlb.gl_accounts_used_import view did the same).
*/
with accounts_used as (
    select distinct account from {{ ref('stg_hlb__gl') }}
),
ranked as (
    select
        a.account_number,
        a.account_name,
        a.account_full_name,
        a.parent_account_name,
        a.account_type,
        a.account_type_detail,
        a.updated_at,
        -- Source occasionally has duplicate account_number rows where one
        -- is marked deleted. Keep the live one.
        row_number() over (
            partition by a.account_number
            order by case when a.account_name like '%(deleted)%' then 1 else 0 end
        ) as rn
    from {{ source('bronze_hlb', 'gl_accounts_import') }} a
    inner join accounts_used u on a.account_number = u.account
)
select
    account_number as account,
    account_name,
    account_full_name,
    parent_account_name,
    account_type,
    account_type_detail,
    updated_at
from ranked
where rn = 1
