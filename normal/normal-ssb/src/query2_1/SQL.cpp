//
// Created by matt on 10/8/20.
//

#include "normal/ssb/query2_1/SQL.h"

#include <fmt/format.h>

using namespace normal::ssb::query2_1;

std::string SQL::partFilter(const std::string& category, const std::string &catalogue) {

  auto sql = fmt::format(
	  "select "
	  "* "
	  "from "
	  "{0}.part "
	  "where "
	  "p_category = '{1}'; ",
	  catalogue,
	  category
  );

  return sql;
}

std::string SQL::join2x(const std::string& region, const std::string &catalogue) {

  auto sql = fmt::format(
	  "select "
	  "* "
	  "from "
	  "{0}.lineorder, {0}.supplier "
	  "where "
	  "lo_suppkey = s_suppkey "
	  "and s_region = '{1}'; ",
	  catalogue,
	  region
  );

  return sql;
}

std::string SQL::join3x(const std::string& region, const std::string &catalogue) {

  auto sql = fmt::format(
	  "select "
	  "* "
	  "from "
	  "{0}.lineorder, {0}.date, {0}.supplier "
	  "where "
	  "lo_orderdate = d_datekey "
	  "and lo_suppkey = s_suppkey "
	  "and s_region = '{1}'; ",
	  catalogue,
	  region
  );

  return sql;
}

std::string SQL::join(const std::string& category, const std::string& region, const std::string &catalogue) {

  auto sql = fmt::format(
	  "select "
	  "* "
	  "from "
	  "{0}.lineorder, {0}.date, {0}.part, {0}.supplier "
	  "where "
	  "lo_orderdate = d_datekey "
	  "and lo_partkey = p_partkey "
	  "and lo_suppkey = s_suppkey "
	  "and p_category = '{1}' "
	  "and s_region = '{2}'; ",
	  catalogue,
	  category,
	  region
  );

  return sql;
}
