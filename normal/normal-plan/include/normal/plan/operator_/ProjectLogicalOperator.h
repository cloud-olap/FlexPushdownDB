//
// Created by matt on 14/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_PROJECTLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_PROJECTLOGICALOPERATOR_H

#include <memory>
#include <vector>

#include <normal/core/Operator.h>
#include <normal/expression/Expression.h>
#include <normal/expression/gandiva/Expression.h>

#include <normal/plan/operator_/LogicalOperator.h>

namespace normal::plan::operator_ {

class ProjectLogicalOperator : public LogicalOperator {

public:
  explicit ProjectLogicalOperator(std::shared_ptr<std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>> expressions,
                                  std::shared_ptr<LogicalOperator> producer_);

  [[nodiscard]] const std::shared_ptr<std::vector<std::shared_ptr<expression::gandiva::Expression>>> &expressions() const;

  std::shared_ptr<std::vector<std::shared_ptr<core::Operator>>> toOperators() override;

  const std::shared_ptr<LogicalOperator> &getProducer() const;

  void setNumConcurrentUnits(int numConcurrentUnits);

private:
  std::shared_ptr<std::vector<std::shared_ptr<expression::gandiva::Expression>>> expressions_;

  std::shared_ptr<LogicalOperator> producer_;
  int numConcurrentUnits;
};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_PROJECTLOGICALOPERATOR_H
