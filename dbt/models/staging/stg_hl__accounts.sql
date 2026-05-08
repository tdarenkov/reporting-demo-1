/* HL chart of accounts (filtered to accounts referenced in GL). */
with used as (
    select distinct account from {{ ref('stg_hl__gl') }}
)
select
    a.account_code as account,
    a.account_name,
    a.account_category,
    a.account_group,
    a.account_type,
    a.updated_at
from {{ source('bronze_hl', 'gl_accounts_import') }} a
inner join used u on a.account_code = u.account
