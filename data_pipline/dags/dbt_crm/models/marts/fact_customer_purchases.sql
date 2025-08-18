SELECT
    pr.product_id,
    pr.product_name,
    pr.category,
    pr.price AS product_price,
    p.purchase_id,
    p.amount AS purchase_amount,
    p.date AS purchase_date,
    c.customer_id,
    c.name AS customer_name,
    c.email,
    c.region
FROM {{ ref('stg_products') }} AS pr
LEFT JOIN {{ ref('stg_purchases') }} AS p
    ON pr.product_id = p.product_id
LEFT JOIN {{ ref('stg_customers') }} AS c
    ON p.customer_id = c.customer_id