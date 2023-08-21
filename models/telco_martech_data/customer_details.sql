with source_customer_details as (
  select * from {{ ref('input') }}
)
select * from source_customer_details
