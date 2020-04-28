//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_IPROJECTOR_H
#define NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_IPROJECTOR_H

#include <arrow/type.h>
#include <arrow/array.h>

namespace normal::expression {

class IProjector {

public:
  virtual ~IProjector() = default;

  virtual std::shared_ptr<arrow::Schema> getResultSchema() = 0;

  virtual std::shared_ptr<arrow::ArrayVector> evaluate(const arrow::RecordBatch &recordBatch) = 0;

  virtual void compile(const std::shared_ptr<arrow::Schema> &schema) = 0;

};

}

#endif //NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_IPROJECTOR_H
