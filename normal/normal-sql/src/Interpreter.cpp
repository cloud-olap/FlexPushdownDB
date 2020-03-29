//
// Created by matt on 26/3/20.
//

#include <Interpreter.h>

#include <normal/sql/NormalSQLLexer.h>
#include <normal/sql/NormalSQLParser.h>
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

  SPDLOG_DEBUG("Finished");
}

void Interpreter::put(const std::shared_ptr<Catalogue> &catalogue) {
  catalogues_->insert(std::pair(catalogue->getName(), catalogue));
}


