SELECT
    c.customer_id,
    c.name AS customer_name,
    c.email,
    c.region,
    p.purchase_id,
    p.amount,
    p.date
FROM {{ ref('stg_customers') }} AS c
JOIN {{ ref('stg_purchases') }} AS p
  ON c.customer_id = p.customer_id