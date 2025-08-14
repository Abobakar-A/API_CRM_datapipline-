-- tests/purchase_amount_is_positive.sql

-- هذا الاختبار سيفشل إذا وجد أي صف تكون فيه قيمة amount أقل من أو تساوي الصفر
select
  purchase_id
from {{ source('crm_source', 'purchases') }}
where amount <= 0