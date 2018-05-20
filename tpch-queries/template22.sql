select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal
		from
			customer
		where
			substring(c_phone from 1 for 2) in
				(CAST('13' as char(15)), 
				 CAST('31' as char(15)), 
				 CAST('23' as char(15)), 
				 CAST('29' as char(15)), 
				 CAST('30' as char(15)), 
				 CAST('18' as char(15)), 
				 CAST('17' as char(15))) 
			and c_acctbal > (
				select
					avg(c_acctbal)
				from
					customer
				where
					c_acctbal > 0.00
					and substring(c_phone from 1 for 2) in
				(CAST('13' as char(15)), 
				 CAST('31' as char(15)), 
				 CAST('23' as char(15)), 
				 CAST('29' as char(15)), 
				 CAST('30' as char(15)), 
				 CAST('18' as char(15)), 
				 CAST('17' as char(15))) 
			)
			and not exists (
				select
					*
				from
					orders
				where
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;
