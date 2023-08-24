with
    raw_csv as (select * from {{ source("raw", "input") }}),

    parsed_customer_details as (
        select
            msisdn,
            subscr_id,
            email_address,
            age,
            parse_date('%m/%d/%Y', join_date) as join_date,
            gender,
            postal_sector,
            handset_model,
            handset_manufacturer,
            needs_segment_name,
            smart_phone_ind,
            operating_system_name,
            lte_subscr_ind,
            bill_cycle_day,
            avg_3_mths_spend,
            avg_3_mths_calls_usage,
            avg_3_mths_sms_usage,
            avg_3_mths_data_usage,
            avg_3_mths_intl_calls_usage,
            avg_3_mths_roam_calls_usage,
            avg_3_mths_roam_sms_usage,
            avg_3_mths_roam_data_usage,
            data_bolton_ind,
            insurance_bolton_ind,
            o2travel_optin_ind,
            pm_registered_ind,
            parse_date('%m/%d/%Y', connection_dt) as connection_dt,
            parse_date('%m/%d/%Y', contract_start_dt) as contract_start_dt,
            parse_date('%m/%d/%Y', contract_end_dt) as contract_end_dt,
            contract_term_mths,
            parse_date('%m/%d/%Y', upgrade_dt) as upgrade_dt,
            cust_tenure_mths,
            pay_and_go_migrated_ind,
            parse_date('%m/%d/%Y', pay_and_go_migrated_dt) as pay_and_go_migrated_dt,
            ported_in_ind,
            parse_date('%m/%d/%Y', ported_in_dt) as ported_in_dt,
            ported_in_from_netwk_name,
            parse_date('%m/%d/%Y', disconnection_dt) as disconnection_dt,
            tariff_name,
            sim_only_ind,
            acquisition_channel_name,
            billing_system_name,
            parse_date('%m/%d/%Y', last_billing_date) as last_billing_date,
            event_desc,
            contact_event_type_cd,
            parse_date('%m/%d/%Y', event_start_dt) as event_start_dt,
            campaign_cd,
            texts_optin_ind,
            email_optin_ind,
            phone_optin_ind,
            post_optin_ind,
            all_marketing_optin_ind

        from raw_csv

        where msisdn is not null and subscr_id is not null and email_address is not null
    )

select *
from parsed_customer_details
