-- using 1511765941 as a seed to the RNG


select
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	part
where
	(
		p_partkey = l_partkey
		and p_brand = cast('Brand#11' as char(10))
		and p_container in (cast('SM CASE' as char(10)), cast('SM BOX' as char(10)), cast('SM PACK' as char(10)), cast('SM PKG' as char(10)))
		and l_quantity >= 3 and l_quantity <= 3 + 10
		and p_size between 1 and 5
		and l_shipmode in (cast('AIR' as char(10)), cast('AIR REG' as char(10)))
		and l_shipinstruct = cast('DELIVER IN PERSON' as char(25))
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = cast('Brand#44' as char(10))
		and p_container in (cast('MED BAG' as char(10)), cast('MED BOX' as char(10)), cast('MED PKG' as char(10)), cast('MED PACK' as char(10)))
		and l_quantity >= 16 and l_quantity <= 16 + 10
		and p_size between 1 and 10
		and l_shipmode in (cast('AIR' as char(10)), cast('AIR REG' as char(10)))
		and l_shipinstruct = cast('DELIVER IN PERSON' as char(25))
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = cast('Brand#53' as char(10))
		and p_container in (cast('LG CASE' as char(10)), cast('LG BOX' as char(10)), cast('LG PACK' as char(10)), cast('LG PKG' as char(10)))
		and l_quantity >= 24 and l_quantity <= 24 + 10
		and p_size between 1 and 15
		and l_shipmode in (cast('AIR' as char(10)), cast('AIR REG' as char(10)))
		and l_shipinstruct = cast('DELIVER IN PERSON' as char(25))
	);

