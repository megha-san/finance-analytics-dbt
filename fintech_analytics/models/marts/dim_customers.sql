-- models/marts/dim_customers.sql

with profiles as (
    select * from {{ ref('int_customer_spend_profile') }}
)

select
    customer_id,
    country,
    age_band,
    risk_tier,
    signup_date,
    lifetime_txn_count,
    lifetime_spend,
    avg_txn_amount,
    first_txn_date,
    last_txn_date,
    days_since_last_txn,
    distinct_merchant_count,
    distinct_card_count,
    txn_count_30d,
    spend_30d,
    txn_count_7d,
    fraud_txn_count,
    fraud_amount,
    round(coalesce(fraud_rate, 0), 4)   as fraud_rate,
    cross_border_txn_count,
    unusual_hour_txn_count,
    max_single_txn_amount,

    -- Segment label for dashboard drill-through
    case
        when fraud_rate > 0.10 then 'HIGH_FRAUD'
        when fraud_rate > 0.03 then 'ELEVATED_FRAUD'
        else                        'NORMAL'
    end                             as fraud_segment,

    current_timestamp()             as _dbt_updated_at

from profiles
