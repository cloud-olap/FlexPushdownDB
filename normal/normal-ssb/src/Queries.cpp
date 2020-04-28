//
// Created by matt on 28/4/20.
//

#include <normal/ssb/Queries.h>

std::string Queries::query01(short year, short discount, short quantity) {

//  auto sql = fmt::format(
//	  "select "
//	  "sum(lo_extendedprice * lo_discount) as revenue "
//	  "from "
//	  "lineorder, date "
//	  "where "
//	  "lo_orderdate = d_datekey "
//	  "and d_year = {0} -- Specific values below "
//	  "and lo_discount between {1} - 1 "
//	  "and {1} + 1 and lo_quantity < {2}; ",
//	  year,
//	  discount,
//	  quantity
//  );

  // FIXME: using catalogies until default catalogue implemented
  auto sql = fmt::format(
	  "select "
	  "sum(lo_extendedprice * lo_discount) as revenue "
	  "from "
	  "local_fs.lineorder, local_fs.date "
	  "where "
	  "lo_orderdate = d_datekey "
	  "and d_year = {0} -- Specific values below "
	  "and lo_discount between {1} - 1 "
	  "and {1} + 1 and lo_quantity < {2}; ",
	  year,
	  discount,
	  quantity
  );

  return sql;
}
