{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    o.order_date,
    o.order_status,
    p.category,
    count() AS total_orders,
    sum(o.quantity) AS total_units_sold,
    uniqExact(o.customer_id) AS unique_customers,
    round(
        sum(o.quantity * p.unit_price),
        2
    ) AS total_sales_revenue

FROM {{ ref('stg_baserow_orders') }} AS o

LEFT JOIN {{ ref('stg_baserow_products') }} AS p
    ON o.product_id = p.product_id

GROUP BY
    o.order_date,
    o.order_status,
    p.category

ORDER BY
    o.order_date,
    o.order_status,
    p.category