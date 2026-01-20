{{
    config(
        materialized='ephemeral'
        )
}}


with bookings as (
    select 
        BOOKING_ID,
        BOOKING_DATE,
        BOOKING_STATUS,
        CREATED_AT
    FROM {{ ref('obt') }}
)

select * from bookings







