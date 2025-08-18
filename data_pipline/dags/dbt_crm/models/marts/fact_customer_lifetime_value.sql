WITH customer_purchases_summary AS (
    SELECT
        c.customer_id,
        COUNT(p.purchase_id) AS number_of_purchases,
        SUM(p.amount) AS total_customer_spending,
        AVG(p.amount) AS average_purchase_value,
        MIN(p.date) AS first_purchase_date,
        MAX(p.date) AS last_purchase_date
    FROM {{ ref('stg_customers') }} AS c
    LEFT JOIN {{ ref('stg_purchases') }} AS p
        ON c.customer_id = p.customer_id
    GROUP BY
        c.customer_id
)

SELECT
    s.customer_id,
    c.name,
    c.email,
    c.region,
    s.number_of_purchases,
    s.total_customer_spending,
    s.average_purchase_value,
    current_date - s.first_purchase_date AS customer_age_in_days,  -- هذا هو السطر المعدّل
    {{ get_customer_segment('s.total_customer_spending') }} AS customer_segment
FROM customer_purchases_summary AS s
LEFT JOIN {{ ref('stg_customers') }} AS c
    ON s.customer_id = c.customer_id