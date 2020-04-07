//
// Created by matt on 26/3/20.
//

#include <Interpreter.h>

#include <normal/sql/NormalSQLLexer.h>
#include <normal/sql/NormalSQLParser.h>
#include <normal/pushdown/Collate.h>
#include "Listener.h"
#include "Globals.h"

Interpreter::Interpreter() :
    catalogues_(std::make_shared<std::unordered_map<std::string, std::shared_ptr<Catalogue>>>()),
    operatorManager_(std::make_shared<normal::core::OperatorManager>())
{}

void Interpreter::parse(const std::string &sql) {

  SPDLOG_DEBUG("Started");

  std::stringstream ss;
  ss << sql;

  antlr4::ANTLRInputStream input(ss);
  normal::sql::NormalSQLLexer lexer(&input);
  antlr4::CommonTokenStream tokens(&lexer);
  normal::sql::NormalSQLParser parser(&tokens);

  antlr4::tree::ParseTree *tree = parser.parse();
  SPDLOG_DEBUG("Parse Tree:\n{}", tree->toStringTree(true));

  Listener listener(this->catalogues_, this->operatorManager_);
//  antlr4::tree::ParseTreeWalker::DEFAULT.walk(&listener, tree);
  auto sqlStatements = tree->accept(&listener);

  auto typedSqlStatements = sqlStatements.as<std::shared_ptr<std::vector<std::shared_ptr<std::vector<std::shared_ptr<ASTNode>>>>>>();

  // TODO: Perhaps support multiple statements in future
  auto firstSQLStatement = typedSqlStatements->at(0);

  for(const auto& astNode: *firstSQLStatement){
    operatorManager_->put(astNode->toOperator());
  }

  for(const auto& astNode: *firstSQLStatement){
    auto op = operatorManager_->getOperator(astNode->name);
    if(astNode->consumer != nullptr){
      auto consumerOp = operatorManager_->getOperator(astNode->consumer->name);
      op->produce(consumerOp);
      consumerOp->consume(op);
    }
  }

  SPDLOG_DEBUG("Finished");
}

void Interpreter::put(const std::shared_ptr<Catalogue> &catalogue) {
  catalogues_->insert(std::pair(catalogue->getName(), catalogue));
}
const std::shared_ptr<normal::core::OperatorManager> &Interpreter::getOperatorManager() const {
  return operatorManager_;
}


