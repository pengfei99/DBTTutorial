with stg_orders as (
    select
        order_id,
        customer_id,
        order_date,
        status
    from {{ ref('orders') }}
)

select * from stg_orders