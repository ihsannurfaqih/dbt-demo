WITH customer AS (
    SELECT * FROM {{ref('stg_cst_retail')}}
),
dates AS (
    SELECT * FROM {{ref('stg_cst_dates')}}
),
products AS (
    SELECT * FROM {{ref('stg_cst_products')}}
),
orders_raw AS (
    SELECT * FROM {{ref('stg_cst_order')}}
),
order_item AS (
    SELECT * FROM {{ref('stg_cst_orders_items')}}
),
stores AS (
    SELECT * FROM {{ref('stg_cst_stores')}}
),
orders AS (
    SELECT
        a.order_id,
        a.customer_id,
        b.product_id,
        c.date_id,
        e.store_id,
        sum(b.quantity * b.price) as amount,
        sum(b.quantity)
    FROM
        orders_raw a
    LEFT JOIN
        order_item b ON (a.order_id = b.order_id)
    LEFT JOIN    
        dates c ON (DATE(a.transaction_date) = c.date)
    LEFT JOIN
        products d ON (b.product_id = d.product_id)
    LEFT JOIN
        stores e ON (a.store_id = e.store_id)
    GROUP BY
        a.order_id,
        a.customer_id,
        b.product_id,
        c.date_id,
        e.store_id
),
final as(
    SELECT * FROM orders 
)

SELECT * FROM final