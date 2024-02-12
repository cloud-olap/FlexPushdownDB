select d_year,
       s_nation,
       p_category,
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
  and s_region = 'AMERICA'
  and (d_year = 1992 or d_year = 1993)
  and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by d_year, s_nation, p_category
order by d_year, s_nation, p_category