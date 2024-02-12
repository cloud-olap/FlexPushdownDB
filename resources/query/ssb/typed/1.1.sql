select sum(cast(lo_extendedprice as int) * cast(lo_discount as int)) as revenue
from lineorder,
     "date"
where lo_orderdate = d_datekey
  and cast(d_year as int) = 1992
  and cast(lo_discount as int) between 1 and 3
  and cast(lo_quantity as int) < 25
