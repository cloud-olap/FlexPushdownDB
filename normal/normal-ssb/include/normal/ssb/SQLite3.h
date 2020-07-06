//
// Created by matt on 18/6/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLITE3_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLITE3_H

#include <vector>
#include <string>
#include <tl/expected.hpp>
#include <memory>

namespace normal::ssb {

/**
 * SQLLite executor
 */
class SQLite3 {

public:

  /**
   * Runs SQL against the given CSV files
   */
  static tl::expected<std::shared_ptr<std::vector<std::vector<std::pair<std::string, std::string>>>>, std::string>
  execute(const std::string& sql, const std::vector<std::string>& files);

};

}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLITE3_H
