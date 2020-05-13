//
// Created by matt on 7/3/20.
//

#include <utility>

#include "normal/pushdown/aggregate/AggregationFunction.h"

namespace normal::pushdown::aggregate {

AggregationFunction::AggregationFunction(std::string columnName) : alias_(std::move(columnName)) {}

const std::string &AggregationFunction::alias() const {
  return alias_;
}

}