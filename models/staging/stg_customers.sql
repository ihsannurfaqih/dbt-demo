WITH CUSTOMERS AS (

    SELECT
        "ID" AS CUSTOMER_ID,
        "FIRST_NAME",
        "LAST_NAME"

    FROM RAW_DATA.CUSTOMERS

)

select * from CUSTOMERS