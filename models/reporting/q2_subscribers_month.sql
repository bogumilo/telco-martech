with
    transformed as (select * from {{ ref("dim_customer_details") }}),

    counter as (

        select j_month, count(*) as subscribers_cnt from transformed group by 1
    ),

    min_max as (

        select
            j_month,

            min(subscribers_cnt) over () as min_subs,
            max(subscribers_cnt) over () as max_subs

        from counter

    )
select
    c.*,
    case when c.subscribers_cnt = m.min_subs then 'Y' else 'N' end as the_worst_month,
    case when c.subscribers_cnt = m.max_subs then 'Y' else 'N' end as the_best_month,
    m.min_subs,
    m.max_subs

from counter c
join min_max m on c.j_month = m.j_month
