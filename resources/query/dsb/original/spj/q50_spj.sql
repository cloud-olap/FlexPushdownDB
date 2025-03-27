
select
   min(s_store_name)
  ,min(s_company_id)
  ,min(s_street_number)
  ,min(s_street_name)
  ,min(s_suite_number)
  ,min(s_city)
  ,min(s_zip)
  ,min(ss_ticket_number)
  ,min(ss_sold_date_sk)
  ,min(sr_returned_date_sk)
  ,min(ss_item_sk)
  ,min(d1.d_date_sk)
from
   store_sales
  ,store_returns
  ,store
  ,date_dim d1
  ,date_dim d2
where
    d2.d_moy = 2
and ss_ticket_number = sr_ticket_number
and ss_item_sk = sr_item_sk
and ss_sold_date_sk   = d1.d_date_sk
and sr_returned_date_sk   = d2.d_date_sk
and ss_customer_sk = sr_customer_sk
and ss_store_sk = s_store_sk
and sr_store_sk = s_store_sk
and d1.d_date between (d2.d_date - interval '120' day (3))
               and d2.d_date
and d1.d_dow = 1
and s_state in ('IN','MS','WI')
