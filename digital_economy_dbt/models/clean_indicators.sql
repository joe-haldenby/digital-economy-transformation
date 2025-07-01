

{{ config(materialized='table') }}

SELECT 
    country_code,
    country_name,
    CAST(date AS INT64) as year,
    indicator,
    indicator_name,
    CAST(value AS FLOAT64) as value,
    -- Add calculated fields
    LAG(value) OVER (
        PARTITION BY country_code, indicator_name 
        ORDER BY date
    ) as previous_year_value,
    -- Calculate year-over-year growth (handle division by zero)
    CASE 
        WHEN LAG(value) OVER (PARTITION BY country_code, indicator_name ORDER BY date) IS NOT NULL
        AND LAG(value) OVER (PARTITION BY country_code, indicator_name ORDER BY date) != 0
        THEN ((value - LAG(value) OVER (PARTITION BY country_code, indicator_name ORDER BY date)) 
              / LAG(value) OVER (PARTITION BY country_code, indicator_name ORDER BY date)) * 100
        ELSE NULL
    END as yoy_growth_rate
FROM {{ source('world_bank_data', 'raw_indicators') }}
WHERE value IS NOT NULL
ORDER BY country_code, indicator_name, year