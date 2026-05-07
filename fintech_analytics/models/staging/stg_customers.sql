-- models/staging/stg_customers.sql

with source as (
    select * from {{ source('raw', 'customers') }}
),

renamed as (
    select
        customer_id,
        trim(first_name)                        as first_name,
        trim(last_name)                         as last_name,
        lower(trim(email))                      as email,
        cast(age as int64)                      as age,
        trim(age_band)                          as age_band,
        upper(trim(country))                    as country,
        trim(city)                              as city,
        cast(signup_date as date)               as signup_date,
        upper(trim(risk_tier))                  as risk_tier,
        cast(is_active as bool)                 as is_active,
        current_timestamp()                     as _loaded_at
    from source
)

select * from renamed
