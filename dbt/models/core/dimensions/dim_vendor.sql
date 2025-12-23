-- models/core/dimensions/dim_vendor.sql
{{ config(
    materialized='table',
    schema='core'
) }}

-- Static vendor dimension based on known NYC TLC vendors
WITH vendors AS (
    VALUES
        (1, 'Creative Mobile Technologies, LLC', 'Third-party vendor for TPEP'),
        (2, 'VeriFone Inc.', 'Third-party vendor for TPEP'),
        (4, 'Square', 'Third-party vendor for FHV')
)

SELECT
    {{ dbt_utils.surrogate_key(['column1']) }} AS vendor_key,
    column1 AS vendor_id,
    column2 AS vendor_name,
    column3 AS vendor_description,
    CURRENT_TIMESTAMP AS created_at
FROM vendors AS t(vendor_id, vendor_name, vendor_description)