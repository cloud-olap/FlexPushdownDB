//
// Created by matt on 6/5/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_COLUMNNAME_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_COLUMNNAME_H

#include <string>
#include <vector>

namespace fpdb::tuple {

class ColumnName {

public:

  /**
   * Converts column name to its canonical form - i.e. lower case
   *
   * @param columnName
   * @return
   */
  static std::string canonicalize(const std::string &columnName);

  static std::vector<std::string> canonicalize(const std::vector<std::string> &columnNames);

};

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_COLUMNNAME_H
