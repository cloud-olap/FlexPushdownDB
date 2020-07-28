//
// Created by matt on 7/3/20.
//

#include <normal/pushdown/aggregate/AggregationResult.h>

using namespace normal::pushdown::aggregate;

AggregationResult::AggregationResult() :
	result_(std::make_shared<std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>>>()) {}

void AggregationResult::put(const std::string &key, const std::shared_ptr<arrow::Scalar> &value) {
  this->result_->insert_or_assign(key, value);
}

std::shared_ptr<arrow::Scalar> AggregationResult::get(const std::string &key) {
  return this->result_->at(key);
}

std::shared_ptr<arrow::Scalar> AggregationResult::get(const std::string &key,
													  const std::shared_ptr<arrow::Scalar> &defaultValue) {
  const auto &res = this->result_->find(key);
  if (res == this->result_->end())
	return defaultValue;
  else
	return res->second;
}

void AggregationResult::reset() {
  this->result_->clear();
}

void AggregationResult::finalize(const std::shared_ptr<arrow::Scalar> &value) {
  this->resultFinal_ = value;
}

std::shared_ptr<arrow::Scalar> AggregationResult::evaluate() {
  return this->resultFinal_;
}
