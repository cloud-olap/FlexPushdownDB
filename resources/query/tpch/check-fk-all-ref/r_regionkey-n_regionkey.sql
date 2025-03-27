select count(*)
from (
  select n.n_regionkey
  from region r left outer join nation n
    on r.r_regionkey = n.n_regionkey
)
where n_regionkey is null;