SELECT
    purchase_id,
    customer_id,
    amount,
    date
FROM {{ source('crm_source', 'purchases') }}