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
  and p_brand1 = 'MFGR#5120'
  and s_region = 'ASIA'
  and (lo_quantity between 17 and 27)
  and (lo_orderdate between 19970101 and 19971231)
group by d_yearmonthnum, p_brand1
order by d_yearmonthnum, p_brand1
