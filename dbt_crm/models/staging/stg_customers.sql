SELECT
    customer_id,
    name,
    email,
    region
FROM {{ source('crm_source', 'customers') }}