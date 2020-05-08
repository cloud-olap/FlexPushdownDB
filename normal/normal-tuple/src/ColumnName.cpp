//
// Created by matt on 6/5/20.
//

#include "normal/tuple/ColumnName.h"

using namespace normal::tuple;

std::string ColumnName::canonicalize(const std::string &columnName) {
  std::string canonicalColumnName(columnName);
  std::transform(canonicalColumnName.begin(), canonicalColumnName.end(), canonicalColumnName.begin(), tolower);
  return canonicalColumnName;
}
