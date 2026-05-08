select account_year, account_code, account_name, level,
       parent1, updated_at
from {{ source('bronze_hlp', 'gl_accounts_import') }}
