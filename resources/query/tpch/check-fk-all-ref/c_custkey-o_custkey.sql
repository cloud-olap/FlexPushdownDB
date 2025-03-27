select count(*)
from (
  select o.o_custkey
  from customer c left outer join orders o
    on c.c_custkey = o.o_custkey
)
where o_custkey is null;