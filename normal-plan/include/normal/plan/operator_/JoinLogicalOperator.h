//
// Created by Yifei Yang on 7/14/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_JOINLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_JOINLOGICALOPERATOR_H

#include <string>
#include <normal/expression/gandiva/Expression.h>
#include <normal/plan/operator_/LogicalOperator.h>

namespace normal::plan::operator_ {

class JoinLogicalOperator : public LogicalOperator{

public:
  explicit JoinLogicalOperator(std::string leftColumnName, std::string rightColumnName,
          std::shared_ptr<LogicalOperator> leftProducer,
          std::shared_ptr<LogicalOperator> rightProducer);

  std::shared_ptr<std::vector<std::shared_ptr<core::Operator>>> toOperators() override;

  [[nodiscard]] const std::string &getLeftColumnName() const;
  [[nodiscard]] const std::string &getRightColumnName() const;

  [[nodiscard]] const std::shared_ptr<LogicalOperator> &getLeftProducer() const;

  [[maybe_unused]] [[maybe_unused]] [[nodiscard]] const std::shared_ptr<LogicalOperator> &getRightProducer() const;

  void setNeededColumnNames(const std::set<std::string> &neededColumnNames);
  [[nodiscard]] const std::set<std::string> &getNeededColumnNames() const;

private:
  std::string leftColumnName_;
  std::string rightColumnName_;

  std::shared_ptr<LogicalOperator> leftProducer_;
  std::shared_ptr<LogicalOperator> rightProducer_;

  // columns needed by downstream operators
  std::set<std::string> neededColumnNames_;
};

}


#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_JOINLOGICALOPERATOR_H
