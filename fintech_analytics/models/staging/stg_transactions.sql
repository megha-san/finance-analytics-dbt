-- models/staging/stg_transactions.sql

with source as (

    select * from {{ source('raw', 'transactions') }}

),

renamed as (

    select
        -- keys
        transaction_id,
        card_id,
        customer_id,
        merchant_id,

        -- amounts
        cast(amount as numeric)                     as amount,
        upper(trim(currency))                       as currency,

        -- timestamps
        cast(transaction_ts as timestamp)           as transaction_ts,
        cast(transaction_date as date)              as transaction_date,
        cast(hour_of_day as int64)                  as hour_of_day,
        initcap(lower(trim(day_of_week)))           as day_of_week,

        -- fraud signals
        cast(is_fraudulent as bool)                 as is_fraudulent,
        case
            when is_fraudulent = true
            then coalesce(nullif(trim(fraud_type), ''), 'unknown')
            else null
        end                                         as fraud_type,

        -- flags
        cast(is_cross_border as bool)               as is_cross_border,
        upper(trim(status))                         as status,

        -- metadata
        current_timestamp()                         as _loaded_at

    from source

)

select * from renamed
