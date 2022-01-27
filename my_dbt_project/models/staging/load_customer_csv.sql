with source as (
    select *
    from {{ ref('customer') }}
), stage_customer as (
    select customer_id,
        zipcode,
        city,
        state_code,
        datetime_created::TIMESTAMP AS datetime_created,
        datetime_updated::TIMESTAMP AS datetime_updated,
    from source
)
select *
from stage_customer

