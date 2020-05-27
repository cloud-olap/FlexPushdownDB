//
// Created by matt on 6/5/20.
//

#include "normal/tuple/ColumnName.h"

#include <algorithm>

using namespace normal::tuple;

std::string ColumnName::canonicalize(const std::string &columnName) {
  std::string canonicalColumnName(columnName);
  std::transform(canonicalColumnName.begin(), canonicalColumnName.end(), canonicalColumnName.begin(), tolower);
  return canonicalColumnName;
}

std::vector<std::string> ColumnName::canonicalize(const std::vector<std::string> &columnNames) {
  std::vector<std::string> canonicalColumnNames;
  for (const auto &columnName: columnNames) {
	canonicalColumnNames.push_back(canonicalize(columnName));
  }
  return canonicalColumnNames;
}
