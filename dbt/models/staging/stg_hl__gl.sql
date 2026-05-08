/* HL GL detail. */
select
    date,
    account,
    source_journal,
    journal_no,
    sequence_no,
    amount::numeric(14, 2) as amount,
    updated_at
from {{ source('bronze_hl', 'gl_current_import') }}
