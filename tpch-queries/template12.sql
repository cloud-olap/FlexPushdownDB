-- using 1511765941 as a seed to the RNG


select
	l_shipmode,
	sum(case
		when o_orderpriority = cast('1-URGENT' as char(15))
			or o_orderpriority = cast('2-HIGH' as char(15))
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> cast('1-URGENT' as char(15)) 
			and o_orderpriority <> cast('2-HIGH' as char(15))
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in (cast('RAIL' as char(10)), cast('AIR' as char(10)))
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date '1993-01-01'
	and l_receiptdate < date '1993-01-01' + interval '1' year
group by
	l_shipmode
order by
	l_shipmode;

