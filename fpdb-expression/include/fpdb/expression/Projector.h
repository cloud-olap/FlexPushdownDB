//
// Created by matt on 27/4/20.
//

#ifndef FPDB_FPDB_EXPRESSION_INCLUDE_FPDB_EXPRESSION_PROJECTOR_H
#define FPDB_FPDB_EXPRESSION_INCLUDE_FPDB_EXPRESSION_PROJECTOR_H

#include <arrow/type.h>
#include <arrow/array.h>
#include <fpdb/tuple/TupleSet.h>

using namespace fpdb::tuple;

namespace fpdb::expression {

class Projector {

public:
  virtual ~Projector() = default;

  virtual std::shared_ptr<arrow::Schema> getResultSchema() = 0;

  virtual tl::expected<std::shared_ptr<TupleSet>, std::string> evaluate(const TupleSet &tupleSet) = 0;
  virtual tl::expected<arrow::ArrayVector, std::string> evaluate(const arrow::RecordBatch &recordBatch) = 0;

  virtual tl::expected<void, std::string> compile(const std::shared_ptr<arrow::Schema> &schema) = 0;

};

}

#endif //FPDB_FPDB_EXPRESSION_INCLUDE_FPDB_EXPRESSION_PROJECTOR_H
