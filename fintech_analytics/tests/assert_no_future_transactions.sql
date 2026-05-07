-- tests/assert_no_future_transactions.sql
-- Catches ETL bugs where transaction timestamps are in the future.
-- Realistic — this exact class of bug shows up in prod pipelines regularly.

select
    transaction_id,
    transaction_ts,
    'Transaction timestamp is in the future' as failure_reason

from {{ ref('fct_transactions') }}
where transaction_ts > current_timestamp()
