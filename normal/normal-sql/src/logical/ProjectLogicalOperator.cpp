//
// Created by matt on 14/4/20.
//

#include <normal/sql/logical/ProjectLogicalOperator.h>

#include <normal/pushdown/Project.h>

using namespace normal::sql::logical;

ProjectLogicalOperator::ProjectLogicalOperator(
    std::vector<std::shared_ptr<normal::core::expression::Expression>> expressions) :
    expressions_(std::move(expressions)) {}

const std::vector<std::shared_ptr<normal::core::expression::Expression>> &ProjectLogicalOperator::expressions() const {
  return expressions_;
}

std::shared_ptr<normal::core::Operator> ProjectLogicalOperator::toOperator() {

  // FIXME: Defaulting to name -> proj
  auto projectPhysicalOperator = std::make_shared<normal::pushdown::Project>("proj", expressions_);

  return projectPhysicalOperator;
}
