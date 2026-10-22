WITH source_data AS (
    SELECT
        source_file,
        TRIM(sku) AS sku,
        valid_from,
        COALESCE(
            NULLIF(TRIM(unit_cost), ''),
            NULLIF(TRIM(cost_sar), '')
        ) AS raw_cost,
        TRY_CAST(
            REPLACE(
                TRANSLATE(
                    COALESCE(
                        NULLIF(TRIM(unit_cost), ''),
                        NULLIF(TRIM(cost_sar), '')
                    ),
                    '٠١٢٣٤٥٦٧٨٩٫',
                    '0123456789.'
                ),
                ',', '.'
            ) AS DOUBLE
        ) AS parsed_cost
    FROM {{ source('raw', 'supplier_prices') }}
),

classified AS (
    SELECT
        s.source_file,
        s.sku,
        TRY_CAST(s.valid_from AS DATE) AS valid_from,
        s.raw_cost,
        CASE
            WHEN p.sku IS NULL THEN 'unknown_sku'
            WHEN s.parsed_cost IS NULL OR s.parsed_cost <= 0
                THEN 'invalid_cost'
        END AS reason
    FROM source_data s
    LEFT JOIN {{ source('raw', 'products') }} p
        ON TRIM(p.sku) = s.sku
)

SELECT
    source_file,
    sku,
    valid_from,
    raw_cost,
    reason
FROM classified
WHERE reason IS NOT NULL