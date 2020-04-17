//
// Created by matt on 26/3/20.
//

#include <normal/sql/Interpreter.h>

#include <normal/sql/NormalSQLLexer.h>
#include <normal/sql/NormalSQLParser.h>
#include <normal/sql/Globals.h>
#include <normal/plan/LogicalPlan.h>
#include <normal/plan/Planner.h>

#include "visitor/Visitor.h"

using namespace normal::sql;

Interpreter::Interpreter() :
    catalogues_(std::make_shared<std::unordered_map<std::string, std::shared_ptr<connector::Catalogue>>>()),
    operatorManager_(std::make_shared<normal::core::OperatorManager>())
{}

void Interpreter::parse(const std::string &sql) {

  SPDLOG_DEBUG("Started");

  std::stringstream ss;
  ss << sql;

  antlr4::ANTLRInputStream input(ss);
  NormalSQLLexer lexer(&input);
  antlr4::CommonTokenStream tokens(&lexer);
  NormalSQLParser parser(&tokens);

  antlr4::tree::ParseTree *tree = parser.parse();
  SPDLOG_DEBUG("Parse Tree:\n{}", tree->toStringTree(true));

  visitor::Visitor visitor(this->catalogues_, this->operatorManager_);
  auto untypedLogicalPlans = tree->accept(&visitor);
  auto logicalPlans = untypedLogicalPlans.as<std::shared_ptr<std::vector<std::shared_ptr<LogicalPlan>>>>();

  // TODO: Perhaps support multiple statements in future
  logicalPlan_ = logicalPlans->at(0);

  // Create physical plan
  auto physicalPlan = Planner::generate(logicalPlan_);

  // Add the plan to the operator manager
  for(const auto& physicalOperator: *physicalPlan->getOperators()){
    operatorManager_->put(physicalOperator.second);
  }

  SPDLOG_DEBUG("Finished");
}

void Interpreter::put(const std::shared_ptr<connector::Catalogue> &catalogue) {
  catalogues_->insert(std::pair(catalogue->getName(), catalogue));
}

const std::shared_ptr<normal::core::OperatorManager> &Interpreter::getOperatorManager() const {
  return operatorManager_;
}

const std::shared_ptr<LogicalPlan> &Interpreter::getLogicalPlan() const {
  return logicalPlan_;
}


