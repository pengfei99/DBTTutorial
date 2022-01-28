with stg_customers as (
    select
        customer_id,
        first_name,
        last_name
    from {{ ref('customers') }}
)

select * from stg_customers