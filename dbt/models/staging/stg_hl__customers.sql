/* HL customers. */
select
    customer_id,
    customer_name,
    coalesce(customer_country, 'No Country') as customer_country,
    updated_at
from {{ source('bronze_hl', 'customers_import') }}
