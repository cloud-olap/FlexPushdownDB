select d_year,
       s_city,
       p_brand1,
       sum(cast(lo_revenue as int) - cast(lo_supplycost as int)) as profit
from "date",
     lineorder,
     supplier,
     customer,
     part
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