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
  Listener listener(this->catalogues_, this->operatorManager_);
  antlr4::tree::ParseTreeWalker::DEFAULT.walk(&listener, tree);

  for(const auto& astNode: listener.symbolTable.table){
    operatorManager_->put(astNode.second->toOperator());
  }

  for(const auto& astNode: listener.symbolTable.table){
    auto op = operatorManager_->getOperator(astNode.second->name);
    if(astNode.second->consumer != nullptr){
      auto consumerOp = operatorManager_->getOperator(astNode.second->consumer->name);
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


