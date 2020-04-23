//
// Created by matt on 7/3/20.
//

#include <sstream>
#include <utility>
#include <normal/expression/Expressions.h>
#include <normal/core/arrow/ScalarHelperImpl.h>
#include <normal/core/arrow/ScalarHelperBuilder.h>

#include "normal/pushdown/aggregate/Sum.h"
#include "normal/pushdown/Globals.h"
#include "arrow/api.h"
#include "arrow/visitor_inline.h"
#include "arrow/scalar.h"

namespace normal::pushdown::aggregate {

Sum::Sum(std::string columnName, std::shared_ptr<normal::expression::Expression> expression) :
    AggregationFunction(std::move(columnName)),
    expression_(std::move(expression)) {}


void normal::pushdown::aggregate::Sum::apply(std::shared_ptr<normal::core::TupleSet> tuples) {

  SPDLOG_DEBUG("Data:\n{}", tuples->toString());

  auto resultType = this->expression_->resultType(tuples->table()->schema());

  std::shared_ptr<arrow::Scalar> batchSum = tuples->visit([&](auto accum, auto &batch) -> auto{
    auto expressionsVec = {this->expression_};
	auto projector = std::make_shared<normal::expression::Projector>(expressionsVec);
    auto arrayVector = expression::Expressions::evaluate(projector, batch);
    auto array = arrayVector->at(0);

    // Initialise accumulator
    if(accum == nullptr) {
      if (resultType->id() == arrow::float64()->id()) {
        accum = arrow::MakeScalar(arrow::float64(), 0.0).ValueOrDie();
      } else if (resultType->id() == arrow::int32()->id()) {
        accum = arrow::MakeScalar(arrow::int32(), 0).ValueOrDie();
      } else if (resultType->id() == arrow::int64()->id()) {
        accum = arrow::MakeScalar(arrow::int64(), 0).ValueOrDie();
      } else {
        throw std::runtime_error("Accumulator init for type " + accum->type->name() + " not implemented yet");
      }
    }

    // FIXME: Dont think this if/then else against arrow types is necessary

    auto colType = array->type();
    if (colType->Equals(arrow::Int32Type())) {
      auto typedArray = std::static_pointer_cast<arrow::Int32Array>(array);
      auto typedAccum = std::static_pointer_cast<arrow::Int32Scalar>(accum);
      for (int i = 0; i < batch.num_rows(); ++i) {
        int val = typedArray->Value(i);
        typedAccum->value += val;
      }
    } else if (colType->Equals(arrow::Int64Type())) {
      auto typedArray = std::static_pointer_cast<arrow::Int64Array>(array);
      auto typedAccum = std::static_pointer_cast<arrow::Int64Scalar>(accum);
      for (int i = 0; i < batch.num_rows(); ++i) {
        long val = typedArray->Value(i);
        typedAccum->value += val;
      }
    } else if (colType->Equals(arrow::StringType())) {
      throw std::runtime_error("Can't sum strings, cast first");
    } else if (colType->Equals(arrow::DoubleType())) {
      auto typedArray = std::static_pointer_cast<arrow::DoubleArray>(array);
      auto typedAccum = std::static_pointer_cast<arrow::DoubleScalar>(accum);

      for (int i = 0; i < batch.num_rows(); ++i) {
        double val = typedArray->Value(i);
        typedAccum->value += val;
      }
    }
    else {
      throw std::runtime_error("Unrecognized type " + colType->name());
    }

    return accum;
  });

  // Get the current running sum, initialising it if necessary
  auto currentSum = this->result()->get(SUM_RESULT_KEY, arrow::MakeScalar(0.0));

  auto currentSumWrapped = ScalarHelperBuilder::make(currentSum).value();
  auto batchSumWrapped = ScalarHelperBuilder::make(batchSum).value();

  // FIXME: Implement this operator properly
  currentSumWrapped->operator+=(batchSumWrapped);

  // Store the current running sum away again for the next batch of tuples
  this->result()->put(SUM_RESULT_KEY, currentSumWrapped->asScalar());
}

std::shared_ptr<arrow::DataType> Sum::returnType() {
  return expression_->getReturnType();
}

void Sum::finalize() {
  this->result()->finalize(this->result()->get(SUM_RESULT_KEY));
}

}
