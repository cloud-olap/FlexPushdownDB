//
// Created by matt on 1/4/20.
//

#include "normal/plan/operator_/LogicalOperator.h"

#include <utility>

using namespace normal::plan::operator_;

LogicalOperator::LogicalOperator(std::shared_ptr<type::OperatorType> Type) : type_(std::move(Type)) {}

std::shared_ptr<type::OperatorType> LogicalOperator::type() {
  return type_;
}

const std::string &LogicalOperator::getName() const {
  return name_;
}

const std::shared_ptr<LogicalOperator> &LogicalOperator::getConsumer() const {
  return consumer_;
}

void LogicalOperator::setName(const std::string &Name) {
  name_ = Name;
}

void LogicalOperator::setConsumer(const std::shared_ptr<LogicalOperator> &Consumer) {
  consumer_ = Consumer;
}
