-- models/marts/dim_merchants.sql

with risk as (
    select * from {{ ref('int_merchant_risk_score') }}
)

select
    merchant_id,
    merchant_name,
    category_code,
    country,
    monthly_txn_limit,
    total_txn_count,
    fraud_txn_count,
    fraud_rate,
    total_volume,
    avg_txn_amount,
    max_txn_amount,
    cross_border_count,
    avg_daily_txn_velocity,
    inherent_high_risk,
    high_fraud_rate_flag,
    high_velocity_flag,
    computed_high_risk_flag,

    -- Risk tier label for dashboard
    case
        when computed_high_risk_flag = true and high_velocity_flag = true then 'CRITICAL'
        when computed_high_risk_flag = true                               then 'HIGH'
        when high_fraud_rate_flag    = true                               then 'MEDIUM'
        else                                                                   'LOW'
    end                             as merchant_risk_tier,

    current_timestamp()             as _dbt_updated_at

from risk
