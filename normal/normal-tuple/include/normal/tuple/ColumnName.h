//
// Created by matt on 6/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNNAME_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNNAME_H

#include <string>
#include <algorithm>

namespace normal::tuple {

class ColumnName {

public:

  /**
   * Converts column name to its canonical form - i.e. lower case
   *
   * @param columnName
   * @return
   */
  static std::string canonicalize(const std::string &columnName) {
	std::string canonicalColumnName(columnName);
	std::transform(canonicalColumnName.begin(), canonicalColumnName.end(), canonicalColumnName.begin(), tolower);
	return canonicalColumnName;
  }

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNNAME_H
