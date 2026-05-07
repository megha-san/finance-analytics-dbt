-- models/intermediate/int_customer_spend_profile.sql
-- Aggregates per-customer behavioral features.
-- Mirrors the concept of a fraud feature store — good interview talking point.

with transactions as (
    select * from {{ ref('stg_transactions') }}
    where status = 'COMPLETED'
),

customers as (
    select * from {{ ref('stg_customers') }}
),

spend_agg as (

    select
        t.customer_id,

        -- Overall lifetime metrics
        count(*)                                                as lifetime_txn_count,
        sum(t.amount)                                           as lifetime_spend,
        avg(t.amount)                                           as avg_txn_amount,
        min(t.transaction_date)                                 as first_txn_date,
        max(t.transaction_date)                                 as last_txn_date,
        count(distinct t.merchant_id)                           as distinct_merchant_count,
        count(distinct t.card_id)                               as distinct_card_count,

        -- Rolling 30-day window (relative to latest transaction date in dataset)
        countif(
            t.transaction_date >= date_sub(
                (select max(transaction_date) from {{ ref('stg_transactions') }}),
                interval 30 day
            )
        )                                                       as txn_count_30d,

        sum(case
            when t.transaction_date >= date_sub(
                (select max(transaction_date) from {{ ref('stg_transactions') }}),
                interval 30 day
            ) then t.amount else 0 end
        )                                                       as spend_30d,

        -- Rolling 7-day window
        countif(
            t.transaction_date >= date_sub(
                (select max(transaction_date) from {{ ref('stg_transactions') }}),
                interval 7 day
            )
        )                                                       as txn_count_7d,

        -- Fraud metrics
        countif(t.is_fraudulent = true)                         as fraud_txn_count,
        sum(case when t.is_fraudulent then t.amount else 0 end) as fraud_amount,

        -- Behavioural signals
        countif(t.is_cross_border = true)                       as cross_border_txn_count,
        countif(t.hour_of_day between 0 and 5)                  as unusual_hour_txn_count,
        max(t.amount)                                           as max_single_txn_amount

    from transactions t
    group by t.customer_id

),

final as (

    select
        cu.customer_id,
        cu.country,
        cu.age_band,
        cu.risk_tier,
        cu.signup_date,

        sa.lifetime_txn_count,
        round(sa.lifetime_spend, 2)                             as lifetime_spend,
        round(sa.avg_txn_amount, 2)                             as avg_txn_amount,
        sa.first_txn_date,
        sa.last_txn_date,
        sa.distinct_merchant_count,
        sa.distinct_card_count,
        sa.txn_count_30d,
        round(sa.spend_30d, 2)                                  as spend_30d,
        sa.txn_count_7d,
        sa.fraud_txn_count,
        round(sa.fraud_amount, 2)                               as fraud_amount,
        safe_divide(sa.fraud_txn_count, sa.lifetime_txn_count)  as fraud_rate,
        sa.cross_border_txn_count,
        sa.unusual_hour_txn_count,
        round(sa.max_single_txn_amount, 2)                      as max_single_txn_amount,

        -- Days since last transaction (recency signal)
        date_diff(
            (select max(transaction_date) from {{ ref('stg_transactions') }}),
            sa.last_txn_date,
            day
        )                                                       as days_since_last_txn

    from customers cu
    left join spend_agg sa on cu.customer_id = sa.customer_id

)

select * from final
