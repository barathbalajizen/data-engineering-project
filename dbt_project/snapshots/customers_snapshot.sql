{#
  SNAPSHOT: customer history (the source of dim_customer, SCD Type 2)

  On every run dbt compares the current customers with what it stored last time:
    - a new customer                      -> adds a row
    - a customer whose updated_at changed -> closes the stored row (sets dbt_valid_to) and adds a new row
    - an unchanged customer               -> nothing happens
  strategy='timestamp' means "a row has changed when its updated_at is newer than the stored one".

  The table lives in the `snapshots` schema and is never rebuilt from scratch: it IS the history.
  Limitation: one version per pipeline run (two changes on the same day are stored as one).
#}
{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select * from {{ source('staging', 'customers') }}

{% endsnapshot %}
