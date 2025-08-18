SELECT
    c.region,
    COUNT(DISTINCT c.customer_id) AS number_of_customers,
    COUNT(p.purchase_id) AS number_of_purchases,
    SUM(p.amount) AS total_sales_amount
FROM {{ ref('stg_customers') }} AS c
LEFT JOIN {{ ref('stg_purchases') }} AS p
    ON c.customer_id = p.customer_id
GROUP BY
    c.region
ORDER BY
    total_sales_amount DESC