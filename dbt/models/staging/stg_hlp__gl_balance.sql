select id, line_number, date, direction, account,
       amount::numeric(14, 2) as amount, updated_at
from {{ source('bronze_hlp', 'gl_balance_import') }}
