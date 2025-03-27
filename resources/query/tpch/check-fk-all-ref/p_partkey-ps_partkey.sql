select count(*)
from (
  select ps.ps_partkey
  from part p left outer join partsupp ps
    on p.p_partkey = ps.ps_partkey
)
where ps_partkey is null;