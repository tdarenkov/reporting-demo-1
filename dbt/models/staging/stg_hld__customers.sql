select
    customer_number,
    customer_name,
    iban,
    bic,
    vat_id,
    updated_at
from {{ source('bronze_hld', 'customers_import') }}
