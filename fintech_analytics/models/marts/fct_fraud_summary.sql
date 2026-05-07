-- models/marts/fct_fraud_summary.sql
-- Daily fraud summary —  primary table 
-- Aggregated data

with transactions as (
    select * from {{ ref('fct_transactions') }}
    where status = 'COMPLETED'
)

select
    transaction_date,
    merchant_category,
    customer_country,
    merchant_country,
    fraud_type,
    is_cross_border,
    customer_age_band,
    customer_risk_tier,
    card_type,
    is_unusual_hour,

    -- Volume
    count(*)                                    as total_transactions,
    sum(amount)                                 as total_volume,
    avg(amount)                                 as avg_txn_amount,

    -- Fraud counts
    countif(is_fraudulent = true)               as fraud_count,
    sum(case when is_fraudulent then amount
             else 0 end)                        as fraud_volume,

    -- Rates
    safe_divide(
        countif(is_fraudulent = true),
        count(*)
    )                                           as fraud_rate,

    safe_divide(
        sum(case when is_fraudulent then amount else 0 end),
        sum(amount)
    )                                           as fraud_volume_rate,

    -- Risk signal counts
    countif(card_anomaly_flag = true)           as card_anomaly_count,
    countif(high_risk_merchant_flag = true)     as high_risk_merchant_txn_count,
    countif(composite_risk_score >= 3)          as high_composite_risk_count,

    current_timestamp()                         as _dbt_updated_at

from transactions
group by
    transaction_date,
    merchant_category,
    customer_country,
    merchant_country,
    fraud_type,
    is_cross_border,
    customer_age_band,
    customer_risk_tier,
    card_type,
    is_unusual_hour
