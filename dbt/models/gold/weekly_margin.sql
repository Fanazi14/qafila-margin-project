WITH order_lines AS (
    SELECT
        o.order_id,
        oi.line_no,
        CAST(
            CAST(o.created_at AS TIMESTAMP) + INTERVAL '3 hours'
            AS DATE
        ) AS order_date,
        s.city,
        p.category,
        oi.quantity,
        oi.unit_price,
        oi.discount,
        p.sku,
        ROUND(
            oi.quantity * oi.unit_price - oi.discount, 2
        ) AS revenue_sar
    FROM {{ source('raw', 'orders') }} o
    JOIN {{ source('raw', 'order_items') }} oi
        ON o.order_id = oi.order_id
    JOIN {{ source('raw', 'products') }} p
        ON oi.product_id = p.product_id
    JOIN {{ source('raw', 'stores') }} s
        ON o.store_id = s.store_id
    WHERE o.status = 'delivered'
      AND oi.quantity > 0
      AND CAST(
          CAST(o.created_at AS TIMESTAMP) + INTERVAL '3 hours'
          AS DATE
      ) >= DATE '2026-08-31'
),

costed_lines AS (
    SELECT
        ol.*,
        c.unit_cost_sar,
        CAST(strftime(ol.order_date, '%V') AS INTEGER) AS iso_week
    FROM order_lines ol
    LEFT JOIN {{ ref('product_cost_history') }} c
        ON TRIM(ol.sku) = TRIM(c.sku)
       AND ol.order_date >= c.valid_from
       AND (
           ol.order_date < c.valid_to
           OR c.valid_to IS NULL
       )
)

SELECT
    iso_week,
    city,
    category,
    COUNT(*) AS lines,
    ROUND(SUM(revenue_sar), 2) AS revenue_sar,
    ROUND(
        SUM(CASE WHEN unit_cost_sar IS NOT NULL
            THEN revenue_sar ELSE 0 END), 2
    ) AS costed_revenue_sar,
    ROUND(
        SUM(CASE WHEN unit_cost_sar IS NOT NULL
            THEN quantity * unit_cost_sar ELSE 0 END), 2
    ) AS cost_sar,
    ROUND(
        SUM(CASE WHEN unit_cost_sar IS NOT NULL
            THEN revenue_sar - quantity * unit_cost_sar
            ELSE 0 END), 2
    ) AS margin_sar,
    SUM(
        CASE WHEN unit_cost_sar IS NULL THEN 1 ELSE 0 END
    ) AS uncosted_lines
FROM costed_lines
GROUP BY iso_week, city, category