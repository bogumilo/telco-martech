with transformed as (
    select * from {{ ref("dim_customer_details") }}
),
input as (
    select * from {{ ref("stg_customer_details") }}
)

select
	t.*,
    i.age
from transformed t 
left join input i
 on i.msisdn = t.msisdn

where safe_cast(i.age as INT64 ) < 30
	and current_date() > t.contract_start_dt
	and t.email_optin_ind = 'Y'