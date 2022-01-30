select *
from (
        select customer_st.customer_id
        from {{ ref('customers_with_state') }} customer_st
            left join {{ ref('stg_customers') }} customer on customer_st.customer_id = customer.customer_id
        where customer.customer_id is null
        UNION ALL
        select customer.customer_id
        from {{ ref('stg_customers') }} customer
            left join {{ ref('customers_with_state') }} customer_st on customer_st.customer_id = customer.customer_id
        where customer_st.customer_id is null
    ) check