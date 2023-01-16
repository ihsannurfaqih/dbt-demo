{{ config(materialized='table') }}

WITH CUSTOMERS AS (
    select * from {{ ref('stg_customers') }}
),

ORDERS AS (

    select * from {{ ref('stg_orders') }}

),

CUSTOMER_ORDERS AS (

    SELECT
        CUSTOMER_ID,

        MIN("ORDER_DATE") AS FIRST_ORDER_DATE,
        MAX("ORDER_DATE") AS MOST_RECENT_ORDER_DATE,
        COUNT(ORDER_ID) AS NUMBER_OF_ORDERS

    FROM ORDERS

    GROUP BY 1

),

FINAL AS (

    SELECT
        CUSTOMERS.CUSTOMER_ID,
        CUSTOMERS."FIRST_NAME",
        CUSTOMERS."LAST_NAME",
        CUSTOMER_ORDERS.FIRST_ORDER_DATE,
        CUSTOMER_ORDERS.MOST_RECENT_ORDER_DATE,
        COALESCE(CUSTOMER_ORDERS.NUMBER_OF_ORDERS, 0) AS NUMBER_OF_ORDERS

    FROM CUSTOMERS

    LEFT JOIN CUSTOMER_ORDERS USING (CUSTOMER_ID)

)

SELECT * FROM FINAL