select d_yearmonthnum,
       s_nation,
       p_category,
       sum(lo_revenue - lo_supplycost) as profit
from "date",
     lineorder,
     supplier,
     customer,
     part
where lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_partkey = p_partkey
  and lo_orderdate = d_datekey
  and c_region = 'MIDDLE EAST'
  and s_region = 'MIDDLE EAST'
  and (p_mfgr = 'MFGR#2' or p_mfgr = 'MFGR#3')
  and (lo_discount between 0 and 2)
  and (lo_orderdate between 19920101 and 19921231)
group by d_yearmonthnum, s_nation, p_category
order by d_yearmonthnum, s_nation, p_category
