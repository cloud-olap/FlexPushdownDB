
select min(item1.i_item_sk),
    min(item2.i_item_sk),
    min(s1.ss_ticket_number),
    min(s1.ss_item_sk)
FROM item AS item1,
item AS item2,
store_sales AS s1,
store_sales AS s2,
date_dim,
customer,
customer_address,
customer_demographics
WHERE
item1.i_item_sk < item2.i_item_sk
AND s1.ss_ticket_number = s2.ss_ticket_number
AND s1.ss_item_sk = item1.i_item_sk and s2.ss_item_sk = item2.i_item_sk
AND s1.ss_customer_sk = c_customer_sk
and c_current_addr_sk = ca_address_sk
and c_current_cdemo_sk = cd_demo_sk
AND d_year between 2000 and 2000 + 1
and d_date_sk = s1.ss_sold_date_sk
and item1.i_category in ('Books', 'Shoes')
and item2.i_manager_id between 48 and 67
and cd_marital_status = 'M'
and cd_education_status = 'College'
and s1.ss_list_price between 80 and 94
and s2.ss_list_price between 80 and 94
