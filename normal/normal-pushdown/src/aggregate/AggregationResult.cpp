//
// Created by matt on 7/3/20.
//

#include "normal/pushdown/aggregate/AggregationResult.h"

namespace normal::pushdown::aggregate {

AggregationResult::AggregationResult() :
    result_(std::make_shared<std::map<std::string, std::string>>()) {}

std::string AggregationResult::get(const std::string& columnName) {
  return this->result_->at(columnName);
}

std::string AggregationResult::get(const std::string& columnName, std::string defaultValue) {
  const std::map<std::string, std::string>::iterator &res = this->result_->find(columnName);
  if (res == this->result_->end())
    return defaultValue;
  else
    return res->second;
}

void AggregationResult::put(const std::string& columnName, std::string value) {
  this->result_->insert_or_assign(columnName, value);
}

}
