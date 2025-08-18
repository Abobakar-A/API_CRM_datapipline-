SELECT
    c.customer_id,
    c.name,
    c.email,
    c.region,
    SUM(p.amount) AS total_customer_spending,
    {{ get_customer_segment('SUM(p.amount)') }} AS customer_segment -- هنا يتم استخدام الماكرو
FROM {{ ref('stg_customers') }} AS c
LEFT JOIN {{ ref('stg_purchases') }} AS p
    ON c.customer_id = p.customer_id
GROUP BY
    c.customer_id,
    c.name,
    c.email,
    c.region