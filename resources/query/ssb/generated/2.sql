select c_city,
       s_city,
       d_yearmonthnum,
       sum(lo_revenue) as revenue
from "date",
     lineorder,
     supplier,
     customer
where lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_orderdate = d_datekey
  and c_nation = 'RUSSIA'
  and s_nation = 'RUSSIA'
  and (lo_quantity between 16 and 26)
  and (lo_orderdate between 19920101 and 19921231)
group by c_city, s_city, d_yearmonthnum
order by d_yearmonthnum asc, revenue desc
