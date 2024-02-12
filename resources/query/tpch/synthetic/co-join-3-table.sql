-- require 'lineitem' and 'orders' are co-located at 'l_orderkey = o.o_orderkey'
select
  o.o_orderpriority,
  count(*) as order_count
from
  lineitem l,
  orders o,
  partsupp ps
where
  l.l_orderkey = o.o_orderkey
  and l.l_partkey = ps.ps_partkey
  and o.o_orderdate >= date '1994-01-01'
  and o.o_comment not like '%special%requests%'
  and ps.ps_availqty < 500
group by
  o.o_orderpriority
order by
  o.o_orderpriority
