select
    account,
    account_name,
    date,
    contra_account,
    contra_account_name,
    contents,
    doc_no,
    amount::numeric(14, 2) as amount,
    updated_at
from {{ source('bronze_hld', 'gl_import') }}
