{% set configs = [
    {
        'table': ref('obt'),
        'column': 'GOLD_OBT.BOOKING_ID, GOLD_OBT.LISTING_ID, GOLD_OBT.HOST_ID, GOLD_OBT.TOTAL_AMOUNT, GOLD_OBT.ACCOMMODATES, GOLD_OBT.BEDROOMS, GOLD_OBT.BATHROOMS, GOLD_OBT.PRICE_PER_NIGHT, GOLD_OBT.RESPONSE_RATE',
        'alias' : 'GOLD_OBT'
    },
    {
        'table': ref('listings'),
        'column': "",
        'alias' : 'DIM_LISTINGS',
        'join_condition': 'GOLD_OBT.listing_id = DIM_LISTINGS.listing_id'
    },
    {
        'table': ref('hosts'),
        'column': "",
        'alias' : 'DIM_HOSTS',
        'join_condition': 'GOLD_OBT.host_id = DIM_HOSTS.host_id'
    }
] %}
SELECT
    {{ configs[0]['column'] }}

FROM
    {{ configs[0]['table'] }} AS {{ configs[0]['alias'] }}
    LEFT JOIN {{ configs[1]['table'] }} AS {{ configs[1]['alias'] }}
        ON {{ configs[1]['join_condition'] }}
    LEFT JOIN {{ configs[2]['table'] }} AS {{ configs[2]['alias'] }}
        ON {{ configs[2]['join_condition'] }}
