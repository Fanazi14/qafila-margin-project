WITH source_data AS (
    SELECT *
    FROM {{ source('raw', 'supplier_prices') }}
)

SELECT
    supplier_id,
    TRIM(sku) AS sku,
    product_name_ar,
    unit_cost,
    valid_from,
    min_order_qty,
    cost_sar,
    currency,
    source_file
FROM source_data