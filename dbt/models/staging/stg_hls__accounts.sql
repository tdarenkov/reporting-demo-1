select account_key, account, description, account_type, updated_at
from {{ source('bronze_hls', 'gl_accounts_import') }}
