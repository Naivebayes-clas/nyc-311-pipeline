{{ config(materialized='view') }}

SELECT
    unique_key,
    created_date,
    borough,
    complaint_type,
    status,
    agency,
    date_trunc('month', created_date) AS request_month
FROM {{ source('nyc311', 'raw_311_requests') }}
WHERE borough IS NOT NULL
  AND complaint_type IS NOT NULL
  AND borough != 'UNSPECIFIED'   
