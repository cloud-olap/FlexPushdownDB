//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_PROJECTOR_H
#define NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_PROJECTOR_H

#include <normal/expression/Projector.h>
#include "Expression.h"

namespace normal::expression::simple {

class Projector : public normal::expression::Projector {
public:
  explicit Projector(std::vector<std::shared_ptr<Expression>> Expressions);
  void compile(const std::shared_ptr<arrow::Schema> &schema) override;
  std::shared_ptr<arrow::ArrayVector> evaluate(const arrow::RecordBatch &recordBatch) override;
  [[nodiscard]] std::shared_ptr<arrow::Schema> getResultSchema() override;

private:
  std::vector<std::shared_ptr<Expression>> expressions_;

};

}

#endif //NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_PROJECTOR_H
