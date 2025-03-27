
select min(i_item_sk)
      ,min(w_warehouse_name)
      ,min(d1.d_week_seq)
      ,min(cs_item_sk)
      ,min(cs_order_number)
      ,min(inv_item_sk)
from catalog_sales
join inventory on (cs_item_sk = inv_item_sk)
join warehouse on (w_warehouse_sk=inv_warehouse_sk)
join item on (i_item_sk = cs_item_sk)
join customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk)
join household_demographics on (cs_bill_hdemo_sk = hd_demo_sk)
join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)
join date_dim d2 on (inv_date_sk = d2.d_date_sk)
join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk)
left outer join promotion on (cs_promo_sk=p_promo_sk)
left outer join catalog_returns on (cr_item_sk = cs_item_sk and cr_order_number = cs_order_number)
where d1.d_week_seq = d2.d_week_seq
  and inv_quantity_on_hand < cs_quantity
  and d3.d_date > d1.d_date + interval '3' day
  and hd_buy_potential = '501-1000'
  and d1.d_year = 2002
  and cd_marital_status = 'W'
  and cd_dep_count between 3 and 5
  and i_category IN ('Home', 'Sports', 'Women')
  and cs_wholesale_cost BETWEEN 73 AND 93
