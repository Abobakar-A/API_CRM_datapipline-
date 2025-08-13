-- tests/customer_id_in_purchases_exists.sql

-- هذا الاختبار سيفشل إذا وجد customer_id في جدول purchases لا يوجد في جدول customers
select
  p.customer_id
from {{ source('crm_source', 'purchases') }} as p
left join {{ source('crm_source', 'customers') }} as c on p.customer_id = c.customer_id
where c.customer_id is null