//
// Created by matt on 1/4/20.
//

#include <normal/plan/operator_/LogicalOperator.h>

using namespace normal::plan::operator_;

LogicalOperator::LogicalOperator(std::shared_ptr<type::OperatorType> type) : type_(std::move(type)) {}

std::shared_ptr<type::OperatorType> LogicalOperator::type() {
  return type_;
}

std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> LogicalOperator::extractSegmentKeys() {
  return std::make_shared<std::vector<std::shared_ptr<normal::cache::SegmentKey>>>();
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

void LogicalOperator::setMode(const std::shared_ptr<normal::plan::operator_::mode::Mode> &mode) {
  mode_ = mode;
}

const std::shared_ptr<normal::plan::operator_::mode::Mode> &LogicalOperator::getMode() const {
  return mode_;
}
