//
// Created by matt on 26/3/20.
//

#include "Visitor.h"

#include <normal/core/expression/Cast.h>
#include <normal/core/expression/Column.h>
#include <normal/core/type/Types.h>

#include <normal/sql/logical/ProjectLogicalOperator.h>
#include <normal/sql/logical/CollateLogicalOperator.h>
#include <normal/sql/logical/AggregateLogicalOperator.h>
#include <normal/sql/logical/SumLogicalFunction.h>

using namespace normal::core::type;
using namespace normal::core::expression;

/**
 * SQL parse tree visitor. Converts the parse tree into a logical graph of operators.
 *
 * @param catalogues
 * @param OperatorManager
 */
normal::sql::visitor::Visitor::Visitor(
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<normal::sql::connector::Catalogue>>> catalogues,
    std::shared_ptr<normal::core::OperatorManager> OperatorManager) :
    catalogues_(std::move(catalogues)),
    operatorManager_(std::move(OperatorManager)) {}

/**
 * The root of the parse tree. Our grammar supports multiple statements so we fiddle with it here to collate
 * them here into a vector.
 *
 * @param ctx
 * @return
 */
antlrcpp::Any normal::sql::visitor::Visitor::visitParse(normal::sql::NormalSQLParser::ParseContext *ctx) {

  auto sqlStatements = std::make_shared<std::vector<std::shared_ptr<std::vector<std::shared_ptr<normal::sql::logical::LogicalOperator>>>>>();

  for (const auto &sql_stmt: ctx->sql_stmt_list()) {
    auto sqlStmt = visitSql_stmt_list(sql_stmt);
    sqlStatements->emplace_back(sqlStmt);
  }

  return sqlStatements;
}

antlrcpp::Any normal::sql::visitor::Visitor::visitSql_stmt_list(normal::sql::NormalSQLParser::Sql_stmt_listContext *ctx) {
  auto res = NormalSQLBaseVisitor::visitSql_stmt_list(ctx);
  return res;
}

antlrcpp::Any normal::sql::visitor::Visitor::visitSql_stmt(normal::sql::NormalSQLParser::Sql_stmtContext *ctx) {
  auto res = NormalSQLBaseVisitor::visitSql_stmt(ctx);
  return res;
}

antlrcpp::Any normal::sql::visitor::Visitor::visitFactored_select_stmt(normal::sql::NormalSQLParser::Factored_select_stmtContext *ctx) {
  auto res = NormalSQLBaseVisitor::visitFactored_select_stmt(ctx);
  return res;
}

/**
 * Select_core is the root of the select statement.
 *
 * @param ctx
 * @return
 */
antlrcpp::Any normal::sql::visitor::Visitor::visitSelect_core(normal::sql::NormalSQLParser::Select_coreContext *ctx) {

  auto nodes = std::make_shared<std::vector<std::shared_ptr<normal::sql::logical::LogicalOperator>>>();

  auto collate = std::make_shared<normal::sql::logical::CollateLogicalOperator>();
  collate->name = "collate";
  nodes->emplace_back(collate);

  auto scanNodes = std::make_shared<std::vector<std::shared_ptr<normal::sql::logical::LogicalOperator>>>();

  for (const auto &tableOrSubquery: ctx->table_or_subquery()) {
    antlrcpp::Any tableOrSubquery_Result = visitTable_or_subquery(tableOrSubquery);
    std::shared_ptr<normal::sql::logical::LogicalOperator> node = tableOrSubquery_Result.as<std::shared_ptr<normal::sql::logical::LogicalOperator>>();
    nodes->emplace_back(tableOrSubquery_Result);
    scanNodes->emplace_back(tableOrSubquery_Result);
  }

  bool simpleScan = false;
  bool project = false;
  bool aggregate = false;

  auto aggregateFunctions = std::make_shared<std::vector<std::shared_ptr<normal::sql::logical::AggregateLogicalFunction>>>();
  auto projectExpressions = std::make_shared<std::vector<std::shared_ptr<normal::core::expression::Expression>>>();

  for (const auto &resultColumn: ctx->result_column()) {
    auto resultColumn_Result = visitResult_column(resultColumn);
    if (resultColumn_Result.is<std::string>()) {
      // FIXME: Need to project or push down possibly, we'll just connect scan to collate for now
      simpleScan = true;
    } else if (resultColumn_Result.is<std::shared_ptr<normal::sql::logical::AggregateLogicalFunction>>()) {
      aggregate = true;
	  auto aggregateFunction = resultColumn_Result.as<std::shared_ptr<normal::sql::logical::AggregateLogicalFunction>>();
	  aggregateFunctions->push_back(aggregateFunction);
    } else if (resultColumn_Result.is<std::shared_ptr<Expression>>()) {
      project = true;
      auto projectExpression = resultColumn_Result.as<std::shared_ptr<Expression>>();
	  projectExpressions->push_back(projectExpression);
    }
    else{
      throw std::runtime_error("Not yet implemented");
    }
  }

  // Simple scan
  if(simpleScan){
    for(const auto &scanNode: *scanNodes){
      scanNode->consumer = collate;
    }
  }

  // Projection
  if(project){

	auto projectNode = std::make_shared<normal::sql::logical::ProjectLogicalOperator>(*projectExpressions);
	projectNode->name = "proj";
	nodes->push_back(projectNode);

    for(const auto &scanNode: *scanNodes){
	  scanNode->consumer = projectNode;
	}

    projectNode->consumer = collate;
  }

  // Aggregate query
  if(aggregate){
	auto aggregateNode = std::make_shared<normal::sql::logical::AggregateLogicalOperator>(*aggregateFunctions);
	aggregateNode->name = "agg";
	nodes->push_back(aggregateNode);

	for(const auto &scanNode: *scanNodes){
	  scanNode->consumer = aggregateNode;
	}

	aggregateNode->consumer = collate;
  }

  return nodes;
}

/**
 * Table_or_subquery is the portion of the query after FROM. Mostly deals with establishing scan operators.
 *
 * @param ctx
 * @return
 */
antlrcpp::Any normal::sql::visitor::Visitor::visitTable_or_subquery(normal::sql::NormalSQLParser::Table_or_subqueryContext *ctx) {

  auto catalogueName = ctx->database_name()->any_name()->IDENTIFIER()->getText();
  auto tableName = ctx->table_name()->any_name()->IDENTIFIER()->getText();

  auto catalogue = this->catalogues_->find(catalogueName);
  if (catalogue == this->catalogues_->end())
    throw std::runtime_error("Catalogue '" + catalogueName + "' not found.");

  auto catalogueEntry = catalogue->second->getEntry(tableName);

  auto scanOp = catalogueEntry->toLogicalOperator();

  return std::static_pointer_cast<normal::sql::logical::LogicalOperator>(scanOp);
}

/**
 * An expression can be any element in a select list essentially, including aggregate operations, projections, column
 * functions like cast etc.
 *
 * @param ctx
 * @return
 */
antlrcpp::Any normal::sql::visitor::Visitor::visitExpr(normal::sql::NormalSQLParser::ExprContext *ctx) {
  if (ctx->K_CAST()) {
    auto expressionCtx = ctx->expr(0);
    auto typeNameCtx = ctx->type_name();
    std::shared_ptr<normal::core::expression::Expression> expression = visitExpr(expressionCtx);
    auto type = typed_visitType_name(typeNameCtx);
    return cast(expression, type);
  } else if (ctx->column_name()) {
    return visitColumn_name(ctx->column_name());
  } else if (ctx->function_name()) {
    auto function = visitFunction_name(ctx->function_name());
    auto typedFunction = function.as<std::shared_ptr<normal::sql::logical::AggregateLogicalFunction>>();
    typedFunction->expression(visitExpr(ctx->expr(0)));
    return function;
  } else {
    throw std::runtime_error("Cannot parse expression " + ctx->getText());
  }
}

/**
 * Functions are aggregates or column functions.
 *
 * @param ctx
 * @return
 */
antlrcpp::Any normal::sql::visitor::Visitor::visitFunction_name(normal::sql::NormalSQLParser::Function_nameContext *ctx) {
  if (ctx->any_name()->IDENTIFIER()->toString() == "sum") {
    auto typedFunction = std::make_shared<normal::sql::logical::SumLogicalFunction>();
    return std::static_pointer_cast<normal::sql::logical::AggregateLogicalFunction>(typedFunction);
  }
  else{
    throw std::runtime_error("Unrecognized function " + ctx->any_name()->IDENTIFIER()->toString());
  }
}

/**
 * A column name
 *
 * @param Context
 * @return
 */
antlrcpp::Any normal::sql::visitor::Visitor::visitColumn_name(normal::sql::NormalSQLParser::Column_nameContext *Context) {
  return col(Context->any_name()->IDENTIFIER()->toString());
}

/**
 * A type name, used in cast operations
 *
 * @param Context
 * @return
 */
antlrcpp::Any normal::sql::visitor::Visitor::visitType_name(normal::sql::NormalSQLParser::Type_nameContext *Context) {
  return typed_visitType_name(Context);
}

std::shared_ptr<normal::core::type::Type> normal::sql::visitor::Visitor::typed_visitType_name(normal::sql::NormalSQLParser::Type_nameContext *Context) {
  std::string typeName = Context->name(0)->any_name()->IDENTIFIER()->toString();
  return Types::fromStringType(const_cast<std::string &&>(typeName));
}

/**
 * A result column is a single element in a select list
 *
 * @param ctx
 * @return
 */
antlrcpp::Any normal::sql::visitor::Visitor::visitResult_column(normal::sql::NormalSQLParser::Result_columnContext *ctx) {
  if (ctx->STAR()) {
    return ctx->STAR()->toString();
  } else if (ctx->expr()) {
    return visitExpr(ctx->expr());
  } else {
    throw std::runtime_error("Cannot parse result column " + ctx->getText());
  }
}

