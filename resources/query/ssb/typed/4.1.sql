select d_year,
       c_nation,
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
  and s_region = 'AMERICA'
  and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by d_year, c_nation
order by d_year, c_nation