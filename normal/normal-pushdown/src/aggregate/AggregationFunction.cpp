//
// Created by matt on 7/3/20.
//

#include <utility>

#include "normal/pushdown/aggregate/AggregationFunction.h"

namespace normal::pushdown::aggregate {

AggregationFunction::AggregationFunction(std::string columnName) : columnName_(std::move(columnName)) {}

AggregationFunction::~AggregationFunction() = default;

const std::string &AggregationFunction::columnName() const {
  return columnName_;
}

void AggregationFunction::init(std::shared_ptr<aggregate::AggregationResult> result) {
  this->buffer_ = std::move(result);
}

std::shared_ptr<aggregate::AggregationResult> AggregationFunction::result() {
  return buffer_;
}

}