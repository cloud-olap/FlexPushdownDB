//
// Created by matt on 26/6/20.
//

#include "normal/ssb/query1_1/SQL.h"

using namespace normal::ssb::query1_1;

std::string SQL::query1_1SQLite(short year, short discount, short quantity, const std::string &catalogue) {

  auto sql = fmt::format(
	  "select "
	  "sum(lo_extendedprice * lo_discount) as revenue "
	  "from "
	  "{0}.lineorder, {0}.date "
	  "where "
	  "lo_orderdate = d_datekey "
	  "and cast(d_year as integer) = {1} "
	  "and cast(lo_discount as integer) between {2} and {3} "
	  "and cast(lo_quantity as integer) < {4}; ",
	  catalogue,
	  year,
	  discount - 1,
	  discount + 1,
	  quantity
  );

  return sql;
}

std::string SQL::query1_1LineOrderScanSQLite(const std::string &catalogue) {

  auto sql = fmt::format(
	  "select "
	  "* "
	  "from "
	  "{0}.lineorder;",
	  catalogue
  );

  return sql;
}

std::string SQL::query1_1DateFilterSQLite(short year, const std::string &catalogue) {

  auto sql = fmt::format(
	  "select "
	  "* "
	  "from "
	  "{0}.date "
	  "where "
	  "cast(d_year as integer) = {1} ",
	  catalogue,
	  year
  );

  return sql;
}

std::string SQL::query1_1LineOrderFilterSQLite(short discount, short quantity, const std::string &catalogue) {

  auto sql = fmt::format(
	  "select "
	  "* "
	  "from "
	  "{0}.lineorder "
	  "where "
	  "cast(lo_discount as integer) between {1} and {2} "
	  "and cast(lo_quantity as integer) < {3};",
	  catalogue,
	  discount - 1,
	  discount + 1,
	  quantity
  );

  return sql;
}

std::string SQL::query1_1JoinSQLite(short year, short discount, short quantity, const std::string &catalogue) {

  auto sql = fmt::format(
	  "select "
	  "* "
	  "from "
	  "{0}.lineorder, {0}.date "
	  "where "
	  "lo_orderdate = d_datekey "
	  "and cast(d_year as integer) = {1} "
	  "and cast(lo_discount as integer) between {2} and {3} "
	  "and cast(lo_quantity as integer) < {4}; ",
	  catalogue,
	  year,
	  discount - 1,
	  discount + 1,
	  quantity
  );

  return sql;
}

