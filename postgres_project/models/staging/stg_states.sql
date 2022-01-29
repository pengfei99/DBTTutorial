
with source as (
    select *
    from {{ source('data_mart', 'states') }}
),
renamed as (
    select state_id::INT AS st_ide,
        state_code::VARCHAR(2) AS st_code,
        state_name::VARCHAR(30) AS st_name
    from source
)
select *
from renamed