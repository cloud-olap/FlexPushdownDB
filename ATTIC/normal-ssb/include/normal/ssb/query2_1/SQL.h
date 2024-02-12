//
// Created by matt on 10/8/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY2_1_SQL_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY2_1_SQL_H

#include <string>

namespace normal::ssb::query2_1 {

/**
 * SQL queries for SSB query 2.1
 */
class SQL {

public:

  static std::string partFilter(const std::string &category, const std::string &catalogue);
  static std::string join2x(const std::string& region, const std::string &catalogue);
  static std::string join3x(const std::string& region, const std::string &catalogue);
  static std::string join(const std::string& category, const std::string& region, const std::string &catalogue);


};

}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERY2_1_SQL_H
