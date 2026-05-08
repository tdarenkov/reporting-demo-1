/* Cross-subsidiary sales fact — the report-facing UNION mart. */
select * from {{ ref('fct_hlb__sales') }}
union all
select * from {{ ref('fct_hl__sales') }}
union all
select * from {{ ref('fct_hlc__sales') }}
union all
select * from {{ ref('fct_hld__sales') }}
union all
select * from {{ ref('fct_hlm__sales') }}
union all
select * from {{ ref('fct_hlp__sales') }}
union all
select * from {{ ref('fct_hls__sales') }}
