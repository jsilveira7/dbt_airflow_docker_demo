-- Test: fact_transactions should not have null customer_id
select *
from {{ ref('fact_transactions') }}
where customer_id is null
