//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_PROJECTOR_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_PROJECTOR_H

#include <arrow/api.h>
#include <gandiva/expression.h>
#include <gandiva/projector.h>

#include <normal/expression/gandiva/Expression.h>
#include <normal/expression/Projector.h>
#include <normal/tuple/TupleSet.h>

using namespace normal::tuple;

namespace normal::expression::gandiva {

class Projector : public normal::expression::Projector {

public:
  explicit Projector(std::vector<std::shared_ptr<Expression>> Expressions);

  std::shared_ptr<TupleSet> evaluate(const TupleSet &tupleSet) override;
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
