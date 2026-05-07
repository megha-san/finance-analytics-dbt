-- tests/assert_fraud_rate_below_threshold.sql
-- Custom singular test: fails (returns rows) if overall fraud rate exceeds 5%.
-- This is a BUSINESS LOGIC test, not just a data quality check.
-- Interview talking point: "I drew a distinction between technical tests (nulls,
-- uniqueness) and business-logic tests that catch data drift or pipeline issues
-- that could corrupt downstream fraud scoring."

with fraud_rate as (

    select
        safe_divide(
            countif(is_fraudulent = true),
            count(*)
        ) as overall_fraud_rate

    from {{ ref('fct_transactions') }}
    where status = 'COMPLETED'

)

select
    overall_fraud_rate,
    'Fraud rate exceeds 5% threshold — investigate pipeline or source data' as failure_reason

from fraud_rate
where overall_fraud_rate > 0.05
