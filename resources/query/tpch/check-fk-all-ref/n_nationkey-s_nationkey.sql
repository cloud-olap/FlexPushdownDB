select count(*)
from (
  select s.s_nationkey
  from nation n left outer join supplier s
    on n.n_nationkey = s.s_nationkey
)
where s_nationkey is null;