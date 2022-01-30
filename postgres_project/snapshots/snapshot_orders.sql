{% snapshot orders_snapshot %}

{{
    config(
      target_database='dbt_project',
      target_schema='snapshots',
      unique_key='order_id',

      strategy='check',
      check_cols=['order_status'],
      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('data_mart', 'orders') }}

{% endsnapshot %}