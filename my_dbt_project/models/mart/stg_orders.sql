with stg_orders as (
    select *
    from {{ source('data_mart', 'orders') }}
)

select *
from stg_orders