select count(*)
from (
  select c.c_nationkey
  from nation n left outer join customer c
    on n.n_nationkey = c.c_nationkey
)
where c_nationkey is null;