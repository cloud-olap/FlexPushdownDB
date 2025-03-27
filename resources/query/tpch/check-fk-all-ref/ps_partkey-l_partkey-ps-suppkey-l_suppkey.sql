select count(*)
from (
  select l.l_partkey, l.l_suppkey
  from partsupp ps left outer join lineitem l
    on ps.ps_partkey = l.l_partkey
    and ps.ps_suppkey = l.l_suppkey
)
where l_partkey is null
  or l_suppkey is null;

select count(*)
from (
  select l.l_partkey
  from partsupp ps left outer join lineitem l
    on ps.ps_partkey = l.l_partkey
)
where l_partkey is null;

select count(*)
from (
  select l.l_suppkey
  from partsupp ps left outer join lineitem l
    on ps.ps_suppkey = l.l_suppkey
)
where l_suppkey is null;