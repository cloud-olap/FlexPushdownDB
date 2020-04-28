//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_PROJECTOR_H
#define NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_PROJECTOR_H

#include <arrow/type.h>
#include <arrow/array.h>

namespace normal::expression {

class Projector {

public:
  virtual ~Projector() = default;

  virtual std::shared_ptr<arrow::Schema> getResultSchema() = 0;

  virtual std::shared_ptr<arrow::ArrayVector> evaluate(const arrow::RecordBatch &recordBatch) = 0;

  virtual void compile(const std::shared_ptr<arrow::Schema> &schema) = 0;

};

}

#endif //NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_PROJECTOR_H
