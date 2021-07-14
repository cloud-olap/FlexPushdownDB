select sum(lo_extendedprice * lo_discount) as revenue
from lineorder,
     date
where lo_orderdate = d_datekey
  and d_weeknuminyear = 3
  and d_year = 1992
  and (lo_discount between 5 and 7)
  and (lo_quantity between 26 and 35);