with source_customer_details as (
  select * from {{ source('telco_martech_data'), 'customer_raw' }}
)
select * from source_customer_details
