//
// Created by matt on 1/4/20.
//

#include "normal/plan/LogicalOperator.h"

#include <utility>

normal::plan::LogicalOperator::LogicalOperator(std::shared_ptr<OperatorType> Type) : type_(std::move(Type)) {}

std::shared_ptr<OperatorType> normal::plan::LogicalOperator::type() {
  return type_;
}

const std::string &normal::plan::LogicalOperator::getName() const {
  return name_;
}

const std::shared_ptr<normal::plan::LogicalOperator> &normal::plan::LogicalOperator::getConsumer() const {
  return consumer_;
}

void normal::plan::LogicalOperator::setName(const std::string &Name) {
  name_ = Name;
}

void normal::plan::LogicalOperator::setConsumer(const std::shared_ptr<LogicalOperator> &Consumer) {
  consumer_ = Consumer;
}
