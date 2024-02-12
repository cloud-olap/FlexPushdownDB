//
// Created by matt on 26/6/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_SQL_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_SQL_H

#include <string>

namespace normal::ssb::query1_1 {

/**
 * SQL queries for SSB query 1.1
 *
 * Includes partial queries
 */
class SQL {

public:
  static std::string dateScan(const std::string &catalogue);
  static std::string lineOrderScan(const std::string &catalogue);

  static std::string dateFilter(short year, const std::string &catalogue);
  static std::string lineOrderFilter(short discount, short quantity, const std::string &catalogue);

  static std::string join(short year, short discount, short quantity, const std::string &catalogue);

  static std::string full(short year, short discount, short quantity, const std::string &catalogue);

};

}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY_1_1_SQL_H
