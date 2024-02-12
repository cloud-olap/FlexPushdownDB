select d_year,
       s_city,
       p_brand1,
       sum(lo_revenue - lo_supplycost) as profit
from supplier,
     lineorder,
     "date",
     part,
     customer
where lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_partkey = p_partkey
  and lo_orderdate = d_datekey
  and c_region = 'AMERICA'
  and s_nation = 'UNITED STATES'
  and (d_year = 1992 or d_year = 1993)
  and p_category = 'MFGR#14'
group by d_year, s_city, p_brand1
order by d_year, s_city, p_brand1