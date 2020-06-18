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

class SQLite3 {

public:
  static tl::expected<std::shared_ptr<std::vector<std::pair<std::string, std::string>>>, std::string>
  execute(std::string sql, std::vector<std::string> files);

};

}

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_SQLITE3_H
