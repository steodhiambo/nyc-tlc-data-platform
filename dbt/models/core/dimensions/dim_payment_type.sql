-- models/core/dimensions/dim_payment_type.sql
{{ config(
    materialized='table',
    schema='core'
) }}

-- Static payment type dimension
WITH payment_types AS (
    VALUES
        (1, 'Credit Card', 'Credit Card'),
        (2, 'Cash', 'Cash'),
        (3, 'No Charge', 'Other'),
        (4, 'Dispute', 'Other'),
        (5, 'Unknown', 'Other'),
        (6, 'Voided Trip', 'Other')
)

SELECT
    {{ dbt_utils.surrogate_key(['column1']) }} AS payment_key,
    column1 AS payment_type_id,
    column2 AS payment_type_description,
    column3 AS payment_type_category,
    CURRENT_TIMESTAMP AS created_at
FROM payment_types AS t(payment_type_id, payment_type_description, payment_type_category)