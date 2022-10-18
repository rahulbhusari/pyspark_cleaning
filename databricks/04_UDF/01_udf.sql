
-- simple SQL UDF
CREATE OR REPLACE FUNCTION sale_announcement(item_name STRING, item_price INT)
RETURNS STRING
RETURN concat("The ", item_name, " is on sale for $", round(item_price * 0.8, 0));

SELECT *, sale_announcement(name,price) AS message
FROM item_lookup

/*

M_PREM_Q    Premium Queen Mattress  1795    The Premium Queen Mattress is on sale for $1436
M_STAN_F    Standard Full Mattress  945     The Standard Full Mattress is on sale for $756
M_PREM_F    Premium Full Mattress   1695    The Premium Full Mattress is on sale for $1356
M_PREM_T    Premium Twin Mattress   1095    The Premium Twin Mattress is on sale for $876

*/

-- simple SQL UDF including a control flow(CASE/WHEN) clause
CREATE OR REPLACE FUNCTION item_preference(name STRING, price INT)
RETURNS STRING
RETURN CASE
    WHEN name = "Standard Queen Mattress" THEN "This is my default mattress"
    WHEN name = "Premium Queen Mattress" THEN "This is my favorite mattress"
    WHEN price > 100 THEN concat("I'd wait until the ", name, " is on sale for $", round(price * 0.8, 0))
    ELSE concat("I don't need a ", name)
END;

SELECT *, item_preference(name,price)
FROM item_lookup

/*
M_PREM_Q    Premium Queen Mattress  1795    This is my favorite mattress
M_STAN_F    Standard Full Mattress  945     I'd wait until the Standard Full Mattress is on sale for $756
M_STAN_Q    Standard Queen Mattress 1045    This is my default mattress
*/