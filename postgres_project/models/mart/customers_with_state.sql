with customers as (
    select *
    from {{ ref('stg_customers') }}
),
states as (
    select *
    from {{ ref('stg_states') }}
)
select c.customer_id,
    c.zipcode,
    c.city,
    c.state_code,
    s.st_name as state_name,
    c.datetime_created,
    c.datetime_updated,
    c.dbt_valid_from::TIMESTAMP as valid_from,
    CASE
        WHEN c.dbt_valid_to IS NULL THEN '9999-12-31'::TIMESTAMP
        ELSE c.dbt_valid_to::TIMESTAMP
    END as valid_to
from customers c
    join states s on c.state_code = s.st_code