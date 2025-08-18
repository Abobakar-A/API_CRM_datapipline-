SELECT
    pr.product_id,
    pr.product_name,
    pr.category,
    pr.price,
    COUNT(p.purchase_id) AS number_of_units_sold,
    SUM(p.amount) AS total_revenue,
    AVG(p.amount) AS average_sale_price,
    COUNT(DISTINCT c.region) AS regions_sold_in,
    MAX(p.date) AS most_recent_sale_date
FROM {{ ref('stg_products') }} AS pr
LEFT JOIN {{ ref('stg_purchases') }} AS p
    ON pr.product_id = p.product_id
LEFT JOIN {{ ref('stg_customers') }} AS c
    ON p.customer_id = c.customer_id
GROUP BY
    pr.product_id,
    pr.product_name,
    pr.category,
    pr.price
ORDER BY
    total_revenue DESC