-- models/intermediate/int_merchant_risk_score.sql
-- Computes per-merchant fraud metrics and a percentile-based risk flag.

with transactions as (
    select * from {{ ref('stg_transactions') }}
    where status = 'COMPLETED'
),

merchants as (
    select * from {{ ref('stg_merchants') }}
),

merchant_agg as (

    select
        merchant_id,
        count(*)                                                    as total_txn_count,
        countif(is_fraudulent = true)                               as fraud_txn_count,
        safe_divide(
            countif(is_fraudulent = true),
            count(*)
        )                                                           as fraud_rate,
        sum(amount)                                                 as total_volume,
        avg(amount)                                                 as avg_txn_amount,
        max(amount)                                                 as max_txn_amount,
        countif(is_cross_border = true)                             as cross_border_count,

        -- Transaction velocity: avg daily transactions
        safe_divide(
            count(*),
            date_diff(max(transaction_date), min(transaction_date), day) + 1
        )                                                           as avg_daily_txn_velocity

    from transactions
    group by merchant_id

),

percentiles as (

    -- Compute 95th percentile fraud rate across all merchants
    select
        percentile_cont(fraud_rate, 0.95) over ()                   as p95_fraud_rate,
        percentile_cont(avg_daily_txn_velocity, 0.95) over ()       as p95_velocity,
        merchant_id
    from merchant_agg

),

final as (

    select
        m.merchant_id,
        m.merchant_name,
        m.category_code,
        m.country,
        m.is_high_risk                                              as inherent_high_risk,
        m.monthly_txn_limit,

        coalesce(ma.total_txn_count, 0)                             as total_txn_count,
        coalesce(ma.fraud_txn_count, 0)                             as fraud_txn_count,
        round(coalesce(ma.fraud_rate, 0), 4)                        as fraud_rate,
        round(coalesce(ma.total_volume, 0), 2)                      as total_volume,
        round(coalesce(ma.avg_txn_amount, 0), 2)                    as avg_txn_amount,
        round(coalesce(ma.max_txn_amount, 0), 2)                    as max_txn_amount,
        coalesce(ma.cross_border_count, 0)                          as cross_border_count,
        round(coalesce(ma.avg_daily_txn_velocity, 0), 2)            as avg_daily_txn_velocity,

        -- Computed risk flags
        ma.fraud_rate > p.p95_fraud_rate                            as high_fraud_rate_flag,
        ma.avg_daily_txn_velocity > p.p95_velocity                  as high_velocity_flag,

        -- Final risk label: high if inherently high-risk OR statistically high fraud rate
        (m.is_high_risk = true or ma.fraud_rate > p.p95_fraud_rate) as computed_high_risk_flag

    from merchants m
    left join merchant_agg ma on m.merchant_id = ma.merchant_id
    left join percentiles p   on m.merchant_id = p.merchant_id

)

select * from final
