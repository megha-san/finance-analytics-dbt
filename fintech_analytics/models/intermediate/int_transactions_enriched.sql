-- models/intermediate/int_transactions_enriched.sql
-- Business logic layer: joins all dimensions, adds fraud signals and derived flags.

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

cards as (
    select * from {{ ref('stg_cards') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

merchants as (
    select * from {{ ref('stg_merchants') }}
),

enriched as (

    select
        -- ── Core transaction fields ─────────────────────────────────────────
        t.transaction_id,
        t.transaction_ts,
        t.transaction_date,
        t.hour_of_day,
        t.day_of_week,
        t.amount,
        t.currency,
        t.status,

        -- ── Fraud labels ────────────────────────────────────────────────────
        t.is_fraudulent,
        t.fraud_type,

        -- ── Card context ────────────────────────────────────────────────────
        t.card_id,
        c.card_type,
        c.is_expired                                    as card_is_expired,
        c.is_blocked                                    as card_is_blocked,

        -- ── Customer context ────────────────────────────────────────────────
        t.customer_id,
        cu.country                                      as customer_country,
        cu.age_band                                     as customer_age_band,
        cu.risk_tier                                    as customer_risk_tier,
        cu.signup_date                                  as customer_signup_date,

        -- ── Merchant context ─────────────────────────────────────────────────
        t.merchant_id,
        m.merchant_name,
        m.category_code                                 as merchant_category,
        m.country                                       as merchant_country,
        m.is_high_risk                                  as merchant_is_high_risk,

        -- ── Derived fraud signals ────────────────────────────────────────────
        t.is_cross_border,

        -- Transaction on a blocked/expired card is a strong fraud signal
        (c.is_blocked = true or c.is_expired = true)    as card_anomaly_flag,

        -- High-risk merchant category
        m.is_high_risk                                  as high_risk_merchant_flag,

        -- High-risk customer tier
        cu.risk_tier in ('HIGH', 'VERY_HIGH')           as high_risk_customer_flag,

        -- Unusual hour: 00:00 – 05:00 local time
        t.hour_of_day between 0 and 5                   as is_unusual_hour,

        -- Amount bucketing for dashboard grouping
        case
            when t.amount <    50  then 'micro (<$50)'
            when t.amount <   200  then 'small ($50–$200)'
            when t.amount <  1000  then 'medium ($200–$1k)'
            when t.amount <  5000  then 'large ($1k–$5k)'
            else                        'very_large (>$5k)'
        end                                             as amount_bucket,

        -- Combined risk score (0–5) — useful for dashboard coloring
        (
            cast(t.is_fraudulent as int64)
            + cast(t.is_cross_border as int64)
            + cast(c.is_blocked as int64)
            + cast(m.is_high_risk as int64)
            + cast(cu.risk_tier in ('HIGH', 'VERY_HIGH') as int64)
        )                                               as composite_risk_score

    from transactions t
    left join cards     c  on t.card_id     = c.card_id
    left join customers cu on t.customer_id = cu.customer_id
    left join merchants m  on t.merchant_id = m.merchant_id

)

select * from enriched
