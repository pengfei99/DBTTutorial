with stg_customers as (
    select *
    from {{ source('data_mart', 'customers') }}
)

select *
from stg_customers