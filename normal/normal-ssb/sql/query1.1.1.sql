select lo_orderdate, lo_quantity, d_weeknuminyear
from lineorder,
     date
where lo_orderdate = d_datekey
  and d_year = 1992
  and (lo_discount between 1 and 3)
  and lo_quantity < 25;
