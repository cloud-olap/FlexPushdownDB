select sum(cast(lo_revenue as int)),
       d_year,
       p_brand1
from "date",
     lineorder,
     supplier,
     part
where lo_orderdate = d_datekey
  and lo_partkey = p_partkey
  and lo_suppkey = s_suppkey
  and p_category = 'MFGR#12'
  and s_region = 'AMERICA'
group by d_year, p_brand1
order by d_year, p_brand1