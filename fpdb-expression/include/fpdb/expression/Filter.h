//
// Created by matt on 6/5/20.
//

#ifndef FPDB_FPDB_EXPRESSION_INCLUDE_FPDB_EXPRESSION_FILTER_H
#define FPDB_FPDB_EXPRESSION_INCLUDE_FPDB_EXPRESSION_FILTER_H

#include <fpdb/tuple/Schema.h>
#include <fpdb/tuple/TupleSet.h>
#include <tl/expected.hpp>
#include <memory>

namespace fpdb::expression {

class Filter {

public:
  virtual ~Filter() = default;

  virtual tl::expected<std::shared_ptr<fpdb::tuple::TupleSet>, std::string>
  evaluate(const fpdb::tuple::TupleSet &TupleSet) = 0;

  virtual tl::expected<arrow::ArrayVector, std::string> evaluate(const arrow::RecordBatch &recordBatch) = 0;

  virtual tl::expected<void, std::string> compile(const std::shared_ptr<arrow::Schema> &inputSchema,
                                                  const std::shared_ptr<arrow::Schema> &outputSchema) = 0;

};

}

#endif //FPDB_FPDB_EXPRESSION_INCLUDE_FPDB_EXPRESSION_FILTER_H
