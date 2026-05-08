/*
    Singular test: every transaction_id (composed of original
    transaction_id + line_order) must be unique in fct_hlb__sales.
    Returns failing rows when the grain is violated; empty = pass.
*/
select transaction_id, count(*) as n
from {{ ref('fct_hlb__sales') }}
group by 1
having count(*) > 1
