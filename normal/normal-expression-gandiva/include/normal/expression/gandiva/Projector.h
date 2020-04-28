//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_PROJECTOR_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_PROJECTOR_H

#include <normal/expression/gandiva/Expression.h>

#include <arrow/api.h>
#include <gandiva/expression.h>
#include <gandiva/projector.h>
#include <normal/expression/IProjector.h>

namespace normal::expression::gandiva {

class Projector : public normal::expression::IProjector {

public:
  explicit Projector(std::vector<std::shared_ptr<Expression>> Expressions);

  std::shared_ptr<arrow::ArrayVector> evaluate(const arrow::RecordBatch &recordBatch) override;
  void compile(const std::shared_ptr<arrow::Schema>& schema) override;
  [[nodiscard]] std::shared_ptr<arrow::Schema> getResultSchema() override;

  std::string showString();

private:
  std::vector<std::shared_ptr<Expression>> expressions_;
  std::vector<::gandiva::ExpressionPtr> gandivaExpressions_;
  std::shared_ptr<arrow::Schema> resultSchema_;
  std::shared_ptr<::gandiva::Projector> gandivaProjector_;
};

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_PROJECTOR_H
