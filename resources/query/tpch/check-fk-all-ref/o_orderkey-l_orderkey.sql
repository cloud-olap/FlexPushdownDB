select count(*)
from (
  select l.l_orderkey
  from orders o left outer join lineitem l
    on o.o_orderkey = l.l_orderkey
)
where l_orderkey is null;