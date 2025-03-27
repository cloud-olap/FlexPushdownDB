
select min(ss_item_sk),
    min(ss_ticket_number),
    min(ws_order_number),
    min(c_customer_sk),
    min(cd_demo_sk),
    min(hd_demo_sk)
from
    store_sales,
    web_sales,
    date_dim d1,
    date_dim d2,
    customer,
    inventory,
    store,
    warehouse,
    item,
    customer_demographics,
    household_demographics,
    customer_address
where
    ss_item_sk = i_item_sk
    and ws_item_sk = ss_item_sk
    and ss_sold_date_sk = d1.d_date_sk
    and ws_sold_date_sk = d2.d_date_sk
    and d2.d_date between d1.d_date and (d1.d_date + interval '30' day)
    and ss_customer_sk = c_customer_sk
    and ws_bill_customer_sk = c_customer_sk
    and ws_warehouse_sk = inv_warehouse_sk
    and ws_warehouse_sk = w_warehouse_sk
    and inv_item_sk = ss_item_sk
    and inv_date_sk = ss_sold_date_sk
    and inv_quantity_on_hand >= ss_quantity
    and s_state = w_state
    AND i_category IN ('Home', 'Sports', 'Women')
    and i_manager_id IN (8, 9, 14, 18, 40, 55, 57, 78, 80, 95)
    and c_current_cdemo_sk = cd_demo_sk
    and c_current_hdemo_sk = hd_demo_sk
    and c_current_addr_sk = ca_address_sk
    and ca_state in ('GA', 'KY', 'MI', 'NC', 'NV')
    and d1.d_year = 1999
    and ws_wholesale_cost BETWEEN 73 AND 93
