WITH parsed AS (
    SELECT
        TRIM(sku) AS sku,
        TRY_CAST(valid_from AS DATE) AS valid_from,
        TRY_CAST(
            REPLACE(
                REPLACE(
                    REPLACE(TRIM(cost_sar), '٩', '9'),
                    '٫', '.'
                ),
                ',', '.'
            ) AS DOUBLE
        ) AS unit_cost_sar,
        source_file
    FROM {{ ref('stg_supplier_prices') }}
),

deduplicated AS (
    SELECT *
    FROM parsed
    WHERE sku IS NOT NULL
      AND sku <> ''
      AND valid_from IS NOT NULL
      AND unit_cost_sar > 0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY sku, valid_from
        ORDER BY source_file DESC
    ) = 1
),

versioned AS (
    SELECT
        sku,
        unit_cost_sar,
        valid_from,
        LEAD(valid_from) OVER (
            PARTITION BY sku
            ORDER BY valid_from
        ) AS valid_to
    FROM deduplicated
)

SELECT
    sku,
    ROUND(unit_cost_sar, 2) AS unit_cost_sar,
    valid_from,
    valid_to,
    valid_to IS NULL AS is_current
FROM versioned