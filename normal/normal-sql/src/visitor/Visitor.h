//
// Created by matt on 26/3/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_VISITOR_H
#define NORMAL_NORMAL_SQL_SRC_VISITOR_H

#include <normal/sql/NormalSQLBaseListener.h>
#include <normal/core/OperatorManager.h>
#include <connector/Catalogue.h>
#include <connector/local-fs/LocalFileSystemCatalogueEntry.h>
#include <normal/sql/NormalSQLBaseVisitor.h>
#include "logical/ScanLogicalOperator.h"
#include "normal/core/expression/Column.h"

using namespace normal::core::type;
using namespace normal::core::expression;

class Visitor : public normal::sql::NormalSQLBaseVisitor {

private:
  std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<Catalogue>>> catalogues_;
  std::shared_ptr<normal::core::OperatorManager> operatorManager_;

public:
  explicit Visitor(
      std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<Catalogue>>> catalogues,
      std::shared_ptr<normal::core::OperatorManager> OperatorManager);
  ~Visitor() override = default;

  antlrcpp::Any visitColumn_name(normal::sql::NormalSQLParser::Column_nameContext *Context) override;
  antlrcpp::Any visitType_name(normal::sql::NormalSQLParser::Type_nameContext *Context) override;
  antlrcpp::Any visitExpr(normal::sql::NormalSQLParser::ExprContext *ctx) override;
  antlrcpp::Any visitSelect_core(normal::sql::NormalSQLParser::Select_coreContext *ctx) override;
  antlrcpp::Any visitTable_or_subquery(normal::sql::NormalSQLParser::Table_or_subqueryContext *ctx) override;
  antlrcpp::Any visitResult_column(normal::sql::NormalSQLParser::Result_columnContext *ctx) override;
  antlrcpp::Any visitParse(normal::sql::NormalSQLParser::ParseContext *ctx) override;
  antlrcpp::Any visitSql_stmt_list(normal::sql::NormalSQLParser::Sql_stmt_listContext *ctx) override;
  antlrcpp::Any visitSql_stmt(normal::sql::NormalSQLParser::Sql_stmtContext *ctx) override;
  antlrcpp::Any visitFactored_select_stmt(normal::sql::NormalSQLParser::Factored_select_stmtContext *ctx) override;
  antlrcpp::Any visitFunction_name(normal::sql::NormalSQLParser::Function_nameContext *ctx) override;

  std::shared_ptr<normal::core::type::Type> typed_visitType_name(normal::sql::NormalSQLParser::Type_nameContext *Context);
};

#endif //NORMAL_NORMAL_SQL_SRC_VISITOR_H
