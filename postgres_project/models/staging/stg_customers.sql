with source as (
    select *
    from {{ ref('customers_snapshot') }}
), stg_customers as (
    select customer_id,
        zipcode,
        city,
        state_code,
        creation_date::TIMESTAMP AS datetime_created,
        update_date::TIMESTAMP AS datetime_updated,
        dbt_valid_from,
        dbt_valid_to
    from source
)
select *
from stg_customers
