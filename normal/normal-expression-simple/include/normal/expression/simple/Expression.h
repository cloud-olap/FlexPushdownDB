//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_EXPRESSION_H
#define NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_EXPRESSION_H

#include <string>
#include <arrow/type.h>
#include <tl/expected.hpp>

#include <normal/expression/Expression.h>

namespace normal::expression::simple {

class Expression : public normal::expression::Expression {

public:
  virtual ~Expression() = default;

  virtual tl::expected<std::shared_ptr<arrow::Array>, std::string> evaluate(const arrow::RecordBatch &batch) = 0;

};

}

#endif //NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_EXPRESSION_H
