select sum(lo_revenue),
       d_yearmonthnum,
       p_brand1
from "date",
     lineorder,
     supplier,
     part
where lo_orderdate = d_datekey
  and lo_partkey = p_partkey
  and lo_suppkey = s_suppkey
  and (p_brand1 between 'MFGR#2110' and 'MFGR#2117')
  and s_region = 'EUROPE'
  and (lo_discount between 1 and 3)
  and (lo_orderdate between 19930101 and 19931231)
group by d_yearmonthnum, p_brand1
order by d_yearmonthnum, p_brand1
