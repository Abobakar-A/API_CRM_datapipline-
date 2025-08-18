SELECT
    product_id,
    product_name,
    category,
    price
FROM {{ source('crm_source', 'products') }}