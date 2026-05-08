CREATE OR REPLACE VIEW `sales.sales_report` AS
SELECT
    s.date,
    s.subsidiary,
    sub.full_name,
    sub.sort_order,
    sub.currency,
    s.fx_amount,
    s.usd_amount,
    s.quantity,
    c.customer_name,
    c.customer_country,
    c.region,
    c.is_ico,
    sk.category_name,
    sk.category_id,
    sk.subcategory_name,
    sk.subcategory_id,
    sk.global_sku,
    sk.global_sku_name,
    ico.sale_from,
    ico.sale_to,
    CASE
        WHEN EXTRACT(MONTH FROM s.date) < sub.max_month
          OR (EXTRACT(MONTH FROM s.date) = sub.max_month
              AND EXTRACT(DAY FROM s.date) <= sub.max_day)
        THEN 'Limited YTD'
        ELSE 'Full Year'
    END AS limited_filter
FROM sales.sales_data s
JOIN sales.subsidiaries sub ON s.subsidiary = sub.subsidiary
JOIN sales.customers c ON s.customer_mapping_id = c.mapping_id
JOIN sales.skus sk ON s.global_sku_id = sk.global_sku_id
LEFT JOIN sales.sales_ico_codes ico ON c.sales_ico_code = ico.sales_ico_code
