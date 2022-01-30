{% snapshot customers_snapshot %}

{{
    config(
      target_database='dbt_project',
      target_schema='snapshots',
      unique_key='customer_id',

      strategy='timestamp',
      updated_at='update_date',
      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('data_mart', 'customers') }}

{% endsnapshot %}