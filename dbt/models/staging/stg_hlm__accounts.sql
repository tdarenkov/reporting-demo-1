select account, account_name, account_type,
       subaccount_1, subaccount_2, updated_at
from {{ source('bronze_hlm', 'gl_accounts_import') }}
