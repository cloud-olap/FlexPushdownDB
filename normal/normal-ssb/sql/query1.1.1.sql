select c_nation, s_nation, d_year, sum(lo_revenue) as revenue
from customer, lineorder, supplier, date
where lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_orderdate = d_datekey
  and c_region = 'EUROPE'
  and s_region = 'EUROPE'
  and d_year >= 1998
  and d_year <= 1998
group by c_nation, s_nation, d_year
order by d_year asc, revenue desc;