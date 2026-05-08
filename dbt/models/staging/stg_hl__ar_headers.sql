/* AR invoice headers — one row per (invoice_no, header_seq_no). */
select
    invoice_no,
    header_seq_no,
    source_journal,
    journal_no,
    invoice_type,
    invoice_date,
    transaction_date,
    customer_id,
    salesperson_id,
    amount_taxable::numeric(14, 2)    as amount_taxable,
    amount_nontaxable::numeric(14, 2) as amount_nontaxable,
    amount_freight::numeric(14, 2)    as amount_freight,
    amount_discount::numeric(14, 2)   as amount_discount,
    updated_at
from {{ source('bronze_hl', 'ar_headers_import') }}
