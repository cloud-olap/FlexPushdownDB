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
#include <normal/expression/Expression.h>

#include <normal/sql/NormalSQLBaseListener.h>
#include <normal/sql/NormalSQLBaseVisitor.h>
#include <normal/connector/Catalogue.h>

using namespace normal::expression;

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
  antlrcpp::Any visitExpr_cast(NormalSQLParser::Expr_castContext *ctx) override;
  antlrcpp::Any visitExpr_literal(NormalSQLParser::Expr_literalContext *ctx) override;
  antlrcpp::Any visitExpr_column(NormalSQLParser::Expr_columnContext *ctx) override;
  antlrcpp::Any visitExpr_function(NormalSQLParser::Expr_functionContext *ctx) override;
  antlrcpp::Any visitExpr_add_sub(NormalSQLParser::Expr_add_subContext *ctx) override;
  antlrcpp::Any visitExpr_mul_div_mod(NormalSQLParser::Expr_mul_div_modContext *ctx) override;
  antlrcpp::Any visitExpr_and(NormalSQLParser::Expr_andContext *ctx) override;
  antlrcpp::Any visitExpr_or(NormalSQLParser::Expr_orContext *ctx) override;
  antlrcpp::Any visitExpr_comp(NormalSQLParser::Expr_compContext *ctx) override;
  antlrcpp::Any visitExpr_eq(NormalSQLParser::Expr_eqContext *ctx) override;
  antlrcpp::Any visitExpr_between(NormalSQLParser::Expr_betweenContext *ctx) override;
  antlrcpp::Any visitExpr_parens(NormalSQLParser::Expr_parensContext *ctx) override;
  antlrcpp::Any visitLiteral_value_numeric(NormalSQLParser::Literal_value_numericContext *ctx) override;
  antlrcpp::Any visitLiteral_value_string(NormalSQLParser::Literal_value_stringContext *ctx) override;
  antlrcpp::Any visitSelect_core(NormalSQLParser::Select_coreContext *ctx) override;
  antlrcpp::Any visitTable_or_subquery(NormalSQLParser::Table_or_subqueryContext *ctx) override;
  antlrcpp::Any visitResult_column(NormalSQLParser::Result_columnContext *ctx) override;
  antlrcpp::Any visitParse(NormalSQLParser::ParseContext *ctx) override;
  antlrcpp::Any visitSql_stmt_list(NormalSQLParser::Sql_stmt_listContext *ctx) override;
  antlrcpp::Any visitSql_stmt(NormalSQLParser::Sql_stmtContext *ctx) override;
  antlrcpp::Any visitSelect_stmt(normal::sql::NormalSQLParser::Select_stmtContext *ctx) override;
  antlrcpp::Any visitFrom_clause(NormalSQLParser::From_clauseContext *ctx) override;
  antlrcpp::Any visitWhere_clause(NormalSQLParser::Where_clauseContext *ctx) override;
  antlrcpp::Any visitGroupBy_clause(NormalSQLParser::GroupBy_clauseContext *ctx) override;
  antlrcpp::Any visitFunction_name(NormalSQLParser::Function_nameContext *ctx) override;

  static std::shared_ptr<arrow::DataType> typed_visitType_name(NormalSQLParser::Type_nameContext *Context);
};

}

#endif //NORMAL_NORMAL_SQL_SRC_VISITOR_H
