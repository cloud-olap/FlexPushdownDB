
select min(c_customer_id),
    min(sr_ticket_number),
    min(sr_item_sk)
 from customer
     ,customer_address
     ,customer_demographics
     ,household_demographics
     ,income_band
     ,store_returns
 where ca_city	        =  'Marion'
   and c_current_addr_sk = ca_address_sk
   and ib_lower_bound   >=  3 * 10000
   and ib_upper_bound   <=  3 * 10000 + 50000
   and ib_income_band_sk = hd_income_band_sk
   and cd_demo_sk = c_current_cdemo_sk
   and hd_demo_sk = c_current_hdemo_sk
   and sr_cdemo_sk = cd_demo_sk
