-- models/marts/fct_transactions.sql
-- Core fact table. Incremental materialization with merge strategy.
-- Partitioned by date, clustered by customer_id + is_fraudulent for query cost efficiency.


{{
    config(
        materialized        = 'incremental',
        incremental_strategy= 'merge',
        unique_key          = 'transaction_id',
        partition_by        = {
            'field'       : 'transaction_date',
            'data_type'   : 'date',
            'granularity' : 'day'
        },
        cluster_by          = ['customer_id', 'is_fraudulent'],
        on_schema_change    = 'sync_all_columns'
    )
}}

with enriched as (

    select * from {{ ref('int_transactions_enriched') }}

    {% if is_incremental() %}
        -- On incremental runs, process only the last 3 days to catch late-arriving data
        where transaction_date >= date_sub(current_date(), interval 3 day)
    {% endif %}

)

select
    -- ── Keys ─────────────────────────────────────────────────────────────────
    transaction_id,
    card_id,
    customer_id,
    merchant_id,

    -- ── Time ─────────────────────────────────────────────────────────────────
    transaction_ts,
    transaction_date,
    hour_of_day,
    day_of_week,

    -- ── Financials ───────────────────────────────────────────────────────────
    amount,
    currency,
    status,
    amount_bucket,

    -- ── Fraud ────────────────────────────────────────────────────────────────
    is_fraudulent,
    fraud_type,

    -- ── Dimensions ───────────────────────────────────────────────────────────
    card_type,
    card_is_expired,
    card_is_blocked,
    customer_country,
    customer_age_band,
    customer_risk_tier,
    merchant_category,
    merchant_country,
    merchant_is_high_risk,

    -- ── Risk signals ─────────────────────────────────────────────────────────
    is_cross_border,
    card_anomaly_flag,
    high_risk_merchant_flag,
    high_risk_customer_flag,
    is_unusual_hour,
    composite_risk_score,

    -- ── Audit ────────────────────────────────────────────────────────────────
    current_timestamp()     as _dbt_updated_at

from enriched
