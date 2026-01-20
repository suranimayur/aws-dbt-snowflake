{{ config(
    materialized = 'incremental',
    unique_key = 'BOOKING_ID'
) }}

SELECT 
    BOOKING_ID,
    LISTING_ID,
    BOOKING_DATE,
    {{ multiply('NIGHTS_BOOKED', 'BOOKING_AMOUNT', 2) }} + CLEANING_FEE + SERVICE_FEE AS TOTAL_AMOUNT,
    BOOKING_STATUS,
    CREATED_AT
FROM {{ ref('bronze_bookings') }}

{% if is_incremental() %}
    {# WHERE BOOKING_DATE > (SELECT max(BOOKING_DATE) FROM {{ this }}) #}
    WHERE BOOKING_DATE > (SELECT COALESCE(MAX(BOOKING_DATE), '1900-01-01') FROM {{ this }})
{% endif %}