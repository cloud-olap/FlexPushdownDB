//
// Created by matt on 7/3/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_FUNCTION_SUM_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_FUNCTION_SUM_H

#include <string>
#include <vector>
#include <normal/core/TupleSet.h>
#include <map>

namespace normal::pushdown::aggregate {

/**
 * Structure for aggregation functions to store intermediate results
 */
class AggregationResult {

private:
  std::shared_ptr<std::map<std::string,std::string>> result_;

public:
  AggregationResult();

  std::string get(const std::string& columnName);
  std::string get(const std::string& columnName, std::string defaultValue);
  void put(const std::string& columnName, std::string value);

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_FUNCTION_SUM_H
