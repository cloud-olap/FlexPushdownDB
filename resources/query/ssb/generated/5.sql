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
  and (c_city = 'UNITED ST3' or c_city = 'UNITED ST6')
  and (s_city = 'UNITED ST3' or s_city = 'UNITED ST6')
  and (lo_quantity between 12 and 22)
  and (lo_orderdate between 19930101 and 19931231)
group by c_city, s_city, d_yearmonthnum
order by d_yearmonthnum asc, revenue desc
