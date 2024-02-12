-- require 'lineitem' and 'orders' are co-located at 'l_orderkey = o.o_orderkey'
select
  o.o_orderpriority,
  count(*) as order_count
from
  lineitem l,
  orders o
where
  l.l_orderkey = o.o_orderkey
  and l.l_commitdate < l.l_receiptdate
  and o.o_orderdate >= date '1994-01-01'
group by
  o.o_orderpriority
order by
  o.o_orderpriority
