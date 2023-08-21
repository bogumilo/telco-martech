with parsed_customer_details as (
  select 
    msisdn,
    subscr_id ,
    email_address,
    age,
    PARSE_DATE('%m/%d/%Y', join_date),
    gender,
    postal_sector,
    handset_model,
    handset_manufacturer,
    needs_segment_name,
    smart_phone_ind ,
    operating_system_name,
    lte_subscr_ind ,
    bill_cycle_day ,
    avg_3_mths_spend FLOAT64,
    avg_3_mths_calls_usage ,
    avg_3_mths_sms_usage ,
    avg_3_mths_data_usage ,
    avg_3_mths_intl_calls_usage ,
    avg_3_mths_roam_calls_usage ,
    avg_3_mths_roam_sms_usage ,
    avg_3_mths_roam_data_usage ,
    data_bolton_ind,
    insurance_bolton_ind,
    o2travel_optin_ind,
    pm_registered_ind,
    PARSE_DATE('%m/%d/%Y', connection_dt) as connection_dt,
    PARSE_DATE('%m/%d/%Y', contract_start_dt) as contract_start_dt,
    PARSE_DATE('%m/%d/%Y', contract_end_dt) as contract_end_dt,
    contract_term_mths ,
    PARSE_DATE('%m/%d/%Y', upgrade_dt) as upgrade_dt,
    cust_tenure_mths ,
    pay_and_go_migrated_ind,
    PARSE_DATE('%m/%d/%Y', pay_and_go_migrated_dt) as pay_and_go_migrated_dt,
    ported_in_ind,
    PARSE_DATE('%m/%d/%Y', ported_in_dt) as ported_in_dt,
    ported_in_from_netwk_name,
    PARSE_DATE('%m/%d/%Y', disconnection_dt) as disconnection_dt,
    tariff_name,
    sim_only_ind,
    acquisition_channel_name,
    billing_system_name,
    PARSE_DATE('%m/%d/%Y', last_billing_date) as last_billing_date,
    event_desc,
    contact_event_type_cd,
    PARSE_DATE('%m/%d/%Y', event_start_dt) as event_start_dt,
    campaign_cd,
    texts_optin_ind,
    email_optin_ind,
    phone_optin_ind,
    post_optin_ind,
    all_marketing_optin_ind
  from {{ sources('telco_martech_data', 'input') }}
)
select
    msisdn,
    email_address,
    CASE
        WHEN age < 18 THEN '<18'
        WHEN age BETWEEN 18 AND 25 THEN '18-25'
        WHEN age BETWEEN 26 AND 35 THEN '26-35'
        WHEN age BETWEEN 36 AND 45 THEN '36-45'
        ELSE '>45'
    END as age_band,
    CASE
        WHEN gender = 'male' THEN 'M'
        WHEN gender = 'female' THEN 'F'
        ELSE 'U'
    END as gender                            
    extract(day from join_date)as j_day,
    extract(month from join_date)as j_month,
    extract(year from join_date)as j_year,
    postal_sector
    handset_model
    handset_manufacturer
    needs_segment_name
    smart_phone_ind
    operating_system_name
    lte_subscr_ind
    bill_cycle_day
    avg_3_mths_spend
    avg_3_mths_calls_usage
    avg_3_mths_sms_usage
    avg_3_mths_data_usage
    avg_3_mths_intl_calls_usage
    avg_3_mths_roam_calls_usage
    avg_3_mths_roam_sms_usage
    avg_3_mths_roam_data_usage
    avg_available
    data_bolton_ind
    insurance_bolton_ind
    o2travel_optin_ind
    pm_registered_ind
    connection_dt
    contract_start_dt
    contract_end_dt
    contract_term_mths
    upgrade_dt
    cust_tenure_mths
    pay_and_go_migrated_ind
    pay_and_go_migrated_dt
    ported_in_ind
    ported_in_dt
    ported_in_from_netwk_name
    disconnection_dt
    tariff_name
    sim_only_ind
    acquisition_channel_name
    billing_system_name
    last_billing_date
    event_desc
    contact_event_type_cd
    event_start_dt
    campaign_cd
    texts_optin_ind
    email_optin_ind
    phone_optin_ind
    post_optin_ind
    all_marketing_optin_ind
from parsed_customer_details
