with transformed as (
    select * from {{ ref("dim_customer_details") }}
),
transformed_quarter as (
    select
        *,
        case when j_month <=3 then 'Q.1'
            when j_month <=6 then 'Q.2'
            when j_month <=9 then 'Q.3'
        else 'Q.4'
        end as quarter

    from transformed
),
age_band_ranked as (
    select
        j_year,
        quarter,
        avg_3_mths_spend,
        age_band,
        dense_rank() over ( partition by j_year, quarter
                            order by avg_3_mths_spend desc
            ) as age_band_rank

    from transformed_quarter
)
select 
    j_year,
    quarter,
    age_band as top_age_bands

from age_band_ranked

where age_band_rank < 2

order by 1, 2