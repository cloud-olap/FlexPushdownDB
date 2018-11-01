with
customer_scan as (
    select *
    from customer
    where c_mktsegment = CAST('BUILDING' AS char(10))
),
order_scan as (
    select *
    from orders
    where o_orderdate < date '1995-03-01'
),
lineitem_scan as (
    select *
    from lineitem
    where l_shipdate > date '1995-03-01'
),
customer_order_join as (
    select *
    from customer_scan, order_scan
    where c_custkey = o_custkey
),
customer_order_lineitem_join as (
    select *
    from lineitem, customer_order_join
    where l_orderkey = o_orderkey
),
group_ as (
    select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate,	o_shippriority
    from customer_order_lineitem_join
    group by l_orderkey, o_orderdate, o_shippriority
),
sort_ as (
    select * from group_ order by revenue desc,	o_orderdate
),
top as (
    select * from sort_ limit 10
)

select * from top