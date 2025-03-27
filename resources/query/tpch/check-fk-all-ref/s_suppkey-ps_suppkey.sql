select count(*)
from (
  select ps.ps_suppkey
  from supplier s left outer join partsupp ps
    on s.s_suppkey = ps.ps_suppkey
)
where ps_suppkey is null;