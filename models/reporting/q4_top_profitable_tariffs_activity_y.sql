with transformed as (
    select * from {{ ref("dim_customer_details") }}
),

tariffs_stats as (
    
    select
        j_year,
        tariff_name,

        sum(avg_3_mths_calls_usage + avg_3_mths_intl_calls_usage + avg_3_mths_roam_calls_usage) as sum_calls, --as calls_comb,
        sum(avg_3_mths_sms_usage + avg_3_mths_roam_sms_usage) as sum_sms, --sms_comb,
        sum(avg_3_mths_data_usage + avg_3_mths_roam_data_usage) as sum_data, --data_comb
    
    from transformed     
    
    group by 1, 2
),

tariffs_ranked as (

    select
        j_year,
        tariff_name,
        dense_rank() over ( partition by j_year
                            order by sum_calls desc
            ) as calls_sum_rank,
        dense_rank() over ( partition by j_year
                            order by sum_sms desc
            ) as sms_sum_rank,            
        dense_rank() over ( partition by j_year
                            order by sum_data desc
            ) as data_sum_rank

    from tariffs_stats
)

select
    j_year,
    'calls' as activity,
    tariff_name as top_tariff_name
from tariffs_ranked
where calls_sum_rank = 1

union all 

select
    j_year,
    'sms' as activity,
    tariff_name as top_tariff_name
from tariffs_ranked
where sms_sum_rank = 1

union all

select
    j_year,
    'data' as activity,
    tariff_name as top_tariff_name
from tariffs_ranked
where data_sum_rank = 1

order by 1, 2