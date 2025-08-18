SELECT
    purchase_id,
    customer_id,
    amount,
    date,
    product_id 
FROM {{ source('crm_source', 'purchases') }}