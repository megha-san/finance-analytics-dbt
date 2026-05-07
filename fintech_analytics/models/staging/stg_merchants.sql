-- models/staging/stg_merchants.sql

with source as (
    select * from {{ source('raw', 'merchants') }}
),

renamed as (
    select
        merchant_id,
        trim(merchant_name)                                     as merchant_name,
        coalesce(nullif(upper(trim(category_code)), ''), 'UNKNOWN') as category_code,
        upper(trim(country))                                    as country,
        trim(city)                                              as city,
        cast(is_high_risk as bool)                              as is_high_risk,
        cast(onboarded_date as date)                            as onboarded_date,
        cast(monthly_txn_limit as int64)                        as monthly_txn_limit,
        current_timestamp()                                     as _loaded_at
    from source
)

select * from renamed
