
select 
   count(distinct ws_order_number) as "order count"
  ,sum(ws_ext_ship_cost) as "total shipping cost"
  ,sum(ws_net_profit) as "total net profit"
from
   web_sales ws1
  ,date_dim
  ,customer_address
  ,web_site
where
    d_date between '2002-6-01' and
           cast('2002-6-01' as date) + interval '60' day
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state in ('AR','GA','IL'
            ,'SD' ,'TX' ,'WI')
and ws1.ws_web_site_sk = web_site_sk
and web_gmt_offset >= -5
and ws1.ws_list_price between 226 and 255
and exists (select *
            from web_sales ws2
            where ws1.ws_order_number = ws2.ws_order_number
              and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
and not exists(select *
               from web_returns wr1
               where ws1.ws_order_number = wr1.wr_order_number
               and wr1.wr_reason_sk in (3, 6, 18, 30, 40)
               )
order by count(distinct ws_order_number)
limit 100
