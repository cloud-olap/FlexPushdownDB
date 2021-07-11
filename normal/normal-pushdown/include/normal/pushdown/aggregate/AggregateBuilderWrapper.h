//
// Created by matt on 27/10/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_AGGREGATEBUILDERWRAPPER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_AGGREGATEBUILDERWRAPPER_H

#include <memory>
#include <utility>

#include <normal/tuple/Scalar.h>
#include "AggregateBuilder.h"

using namespace normal::tuple;

namespace normal::pushdown::aggregate {

template<typename CType, typename ArrowType>
class AggregateBuilderWrapper : public AggregateBuilder {

  using ArrowBuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;

public:
  explicit AggregateBuilderWrapper(std::shared_ptr<ArrowBuilderType> builder) : builder_(std::move(builder)) {}

  tl::expected<void, std::string> append(const std::shared_ptr<AggregationResult> &result) override {
	auto aggregateScalar = result->evaluate();
	auto status = builder_->Append(Scalar::make(aggregateScalar)->value<CType>());
	if (!status.ok())
	  return tl::make_unexpected(status.message());
	return {};
  }

  tl::expected<std::shared_ptr<arrow::Array>, std::string> finalise() override {
	std::shared_ptr<arrow::Array> outputArray;
	auto status = builder_->Finish(&outputArray);
	if (!status.ok())
	  return tl::make_unexpected(status.message());
	return outputArray;
  }

private:
  std::shared_ptr<ArrowBuilderType> builder_;

};

template<>
tl::expected<void, std::string> AggregateBuilderWrapper<std::string, ::arrow::StringType>::append(const std::shared_ptr<
	AggregationResult> &result) {
  auto aggregateScalar = result->evaluate();
  auto status = builder_->Append(Scalar::make(aggregateScalar)->value<std::string>());
  if (!status.ok())
	return tl::make_unexpected(status.message());
  return {};
}

tl::expected<std::shared_ptr<AggregateBuilder>, std::string>
makeAggregateBuilder(const std::shared_ptr<arrow::DataType> &type) {
  switch (type->id()) {
    case arrow::Type::INT16: return std::make_shared<AggregateBuilderWrapper<short, arrow::Int16Type>>(std::make_shared<arrow::Int16Builder>());
    case arrow::Type::INT32: return std::make_shared<AggregateBuilderWrapper<int, arrow::Int32Type>>(std::make_shared<arrow::Int32Builder>());
    case arrow::Type::INT64: return std::make_shared<AggregateBuilderWrapper<long, arrow::Int64Type>>(std::make_shared<arrow::Int64Builder>());
    case arrow::Type::FLOAT: return std::make_shared<AggregateBuilderWrapper<float, arrow::FloatType>>(std::make_shared<arrow::FloatBuilder>());
    case arrow::Type::DOUBLE: return std::make_shared<AggregateBuilderWrapper<double, arrow::DoubleType>>(std::make_shared<arrow::DoubleBuilder>());
    case arrow::Type::STRING: return std::make_shared<AggregateBuilderWrapper<std::string, arrow::StringType>>(std::make_shared<arrow::StringBuilder>());
    default: return tl::make_unexpected(fmt::format("Unrecognized type {}", type->name()));
  }
}

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_AGGREGATEBUILDERWRAPPER_H
