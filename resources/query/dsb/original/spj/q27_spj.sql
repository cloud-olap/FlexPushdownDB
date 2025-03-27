
select min(i_item_id),
        min(s_state),
        min(ss_quantity),
        min(ss_list_price),
        min(ss_coupon_amt),
        min(ss_sales_price),
        min(ss_item_sk),
        min(ss_ticket_number)
 from store_sales, customer_demographics, date_dim, store, item
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_store_sk = s_store_sk and
       ss_cdemo_sk = cd_demo_sk and
       cd_gender = 'F' and
       cd_marital_status = 'D' and
       cd_education_status = 'College' and
       d_year = 2001 and
       s_state = 'IN' and
      i_category  = 'Electronics'
