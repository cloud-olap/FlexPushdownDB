//
// Created by matt on 7/3/20.
//

#include <sstream>
#include <utility>

#include "normal/pushdown/aggregate/Sum.h"
#include "normal/pushdown/Globals.h"

namespace normal::pushdown::aggregate {

Sum::Sum(std::string columnName, std::string inputColumnName) :
    AggregationFunction(std::move(columnName)),
    inputColumnName_(std::move(inputColumnName)) {}

void normal::pushdown::aggregate::Sum::apply(std::shared_ptr<normal::core::TupleSet> tuples) {

  SPDLOG_DEBUG("Data:\n{}", tuples->toString());

  std::string sumString = tuples->visit([&](std::string accum, arrow::RecordBatch &batch) -> std::string {

    auto fieldIndex = batch.schema()->GetFieldIndex(this->inputColumnName());

    if (fieldIndex < 0) {
      throw std::runtime_error("Field '" + this->inputColumnName() + "' not found");
    }

    std::shared_ptr<arrow::Array> array = batch.column(fieldIndex);

    double sum = 0;
    if (accum.empty()) {
      sum = 0;
    } else {
      sum = std::stod(accum);
    }

    std::shared_ptr<arrow::DataType> colType = array->type();
    if (colType->Equals(arrow::Int32Type())) {
      std::shared_ptr<arrow::Int32Array>
          typedArray = std::static_pointer_cast<arrow::Int32Array>(array);
      for (int i = 0; i < batch.num_rows(); ++i) {
        int val = typedArray->Value(i);
        sum += val;
      }
    } else if (colType->Equals(arrow::Int64Type())) {
      std::shared_ptr<arrow::Int64Array>
          typedArray = std::static_pointer_cast<arrow::Int64Array>(array);
      for (int i = 0; i < batch.num_rows(); ++i) {
        long val = typedArray->Value(i);

        // FIXME: This isn't correct
        sum += val;
      }
    } else if (colType->Equals(arrow::StringType())) {
      std::shared_ptr<arrow::StringArray>
          typedArray = std::static_pointer_cast<arrow::StringArray>(array);
      for (int i = 0; i < batch.num_rows(); ++i) {
        std::string val = typedArray->GetString(i);
        sum += std::stod(val);
      }
    } else if (colType->Equals(arrow::DoubleType())) {
      std::shared_ptr<arrow::DoubleArray>
          typedArray = std::static_pointer_cast<arrow::DoubleArray>(array);

      for (int i = 0; i < batch.num_rows(); ++i) {
        double val = typedArray->Value(i);
        sum += val;
      }
    } else {
      abort();
    }

    return std::to_string(sum);
  });


  // FIXME: Too much casting :(
  std::string currentSum = this->result()->get(columnName(), "0");
  double newSum = std::stod(sumString) + std::stod(currentSum);
  this->result()->put(columnName(), std::to_string(newSum));
}

const std::string &Sum::inputColumnName() const {
  return inputColumnName_;
}

}
