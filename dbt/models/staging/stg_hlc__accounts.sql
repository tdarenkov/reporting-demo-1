select
    account,
    account_name,
    account_type,
    updated_at
from {{ source('bronze_hlc', 'gl_accounts_import') }}
