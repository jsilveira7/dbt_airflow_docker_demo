-- Test: fact_transactions must have unique transaction_id
select count(*)
from {{ ref('fact_transactions') }}
group by transaction_id
having count(*) > 1
