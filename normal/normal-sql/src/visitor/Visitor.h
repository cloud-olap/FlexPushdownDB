//
// Created by matt on 26/3/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_VISITOR_H
#define NORMAL_NORMAL_SQL_SRC_VISITOR_H

#include <memory>
#include <unordered_map>
#include <string>

#include <antlr4-runtime.h>

#include <normal/core/OperatorManager.h>
#include <normal/core/type/Type.h>
#include <normal/core/expression/Expression.h>

#include <normal/sql/NormalSQLBaseListener.h>
#include <normal/sql/NormalSQLBaseVisitor.h>
#include <normal/sql/connector/Catalogue.h>

using namespace normal::core::type;
using namespace normal::core::expression;

namespace normal::sql::visitor {

class Visitor : public normal::sql::NormalSQLBaseVisitor {

private:
  std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<connector::Catalogue>>> catalogues_;
  std::shared_ptr<core::OperatorManager> operatorManager_;

public:
  explicit Visitor(
      std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<connector::Catalogue>>> catalogues,
      std::shared_ptr<core::OperatorManager> OperatorManager);
  ~Visitor() override = default;

  antlrcpp::Any visitColumn_name(NormalSQLParser::Column_nameContext *Context) override;
  antlrcpp::Any visitType_name(NormalSQLParser::Type_nameContext *Context) override;
  antlrcpp::Any visitExpr(NormalSQLParser::ExprContext *ctx) override;
  antlrcpp::Any visitSelect_core(NormalSQLParser::Select_coreContext *ctx) override;
  antlrcpp::Any visitTable_or_subquery(NormalSQLParser::Table_or_subqueryContext *ctx) override;
  antlrcpp::Any visitResult_column(NormalSQLParser::Result_columnContext *ctx) override;
  antlrcpp::Any visitParse(NormalSQLParser::ParseContext *ctx) override;
  antlrcpp::Any visitSql_stmt_list(NormalSQLParser::Sql_stmt_listContext *ctx) override;
  antlrcpp::Any visitSql_stmt(NormalSQLParser::Sql_stmtContext *ctx) override;
  antlrcpp::Any visitFactored_select_stmt(NormalSQLParser::Factored_select_stmtContext *ctx) override;
  antlrcpp::Any visitFunction_name(NormalSQLParser::Function_nameContext *ctx) override;

  std::shared_ptr<Type> typed_visitType_name(NormalSQLParser::Type_nameContext *Context);
};

}

#endif //NORMAL_NORMAL_SQL_SRC_VISITOR_H
