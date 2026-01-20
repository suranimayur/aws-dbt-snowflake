{{
  config(
    severity = 'warn'
    )
}}


SELECT 1
from {{ source('staging', 'bookings') }}
where  booking_amount < 2000


