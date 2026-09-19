{{ config(materialized='table') }}

WITH complaint_counts AS (
    SELECT
        borough,
        request_month,
        complaint_type,
        COUNT(*) AS complaint_count,
        RANK() OVER (
            PARTITION BY borough, request_month
            ORDER BY COUNT(*) DESC
        ) AS rank_in_borough
    FROM {{ ref('stg_311_requests') }}
    GROUP BY borough, request_month, complaint_type
)

SELECT
    borough,
    request_month,
    complaint_type,
    complaint_count,
    rank_in_borough
FROM complaint_counts
WHERE rank_in_borough <= 5
ORDER BY borough, request_month, complaint_count DESC   
