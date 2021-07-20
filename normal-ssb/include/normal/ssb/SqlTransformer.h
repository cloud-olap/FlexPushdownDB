//
// Created by Yifei Yang on 3/29/21.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLTRANSFORMER_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLTRANSFORMER_H

#include <string>
#include <vector>
#include <map>

namespace normal::ssb {

std::string transformSqlForPrestoCSV(std::vector<std::string>& sqlLines);

[[maybe_unused]] inline const std::vector<std::pair<std::string, std::string>> typeTransformations {
  {"d_datekey", "int"},
  {"d_yearmonthnum", "int"},
  {"d_year", "int"},
  {"d_daynuminweek", "int"},
  {"d_daynuminmonth", "int"},
  {"d_daynuminyear", "int"},
  {"d_monthnuminyear", "int"},
  {"d_weeknuminyear", "int"},
  {"d_lastdayinweekfl", "boolean"},
  {"d_lastdayinmonthfl", "boolean"},
  {"d_holidayfl", "boolean"},
  {"d_weekdayfl", "boolean"},
  {"s_suppkey", "int"},
  {"p_partkey", "int"},
  {"p_size", "int"},
  {"c_custkey", "int"},
  {"lo_orderkey", "int"},
  {"lo_linenumber", "int"},
  {"lo_custkey", "int"},
  {"lo_partkey", "int"},
  {"lo_suppkey", "int"},
  {"lo_orderdate", "int"},
  {"lo_shippriority", "int"},
  {"lo_quantity", "int"},
  {"lo_extendedprice", "int"},
  {"lo_ordtotalprice", "int"},
  {"lo_discount", "int"},
  {"lo_revenue", "int"},
  {"lo_supplycost", "int"},
  {"lo_tax", "int"},
  {"lo_commitdate", "int"}
};

inline const std::map<std::string, std::string> prefixFields {
  {"d_yearmonthnum", "d_1yearmonthnum"}
};

}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLTRANSFORMER_H
