-- models/staging/stg_cards.sql

with source as (
    select * from {{ source('raw', 'cards') }}
),

renamed as (
    select
        card_id,
        customer_id,
        upper(trim(card_type))                          as card_type,
        card_last4,
        cast(issue_date as date)                        as issue_date,
        cast(expiry_date as date)                       as expiry_date,
        -- Derive is_expired dynamically
        cast(expiry_date as date) < current_date()      as is_expired,
        cast(is_blocked as bool)                        as is_blocked,
        current_timestamp()                             as _loaded_at
    from source
)

select * from renamed
