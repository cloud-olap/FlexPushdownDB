//
// Created by matt on 26/3/20.
//

#include "Visitor.h"

#include <utility>

#include "../Globals.h"
#include <connector/local-fs/LocalFileSystemCatalogueEntry.h>
#include "logical/ScanLogicalOperator.h"
#include "logical/CollateLogicalOperator.h"
#include "logical/AggregateLogicalOperator.h"
#include "normal/core/expression/Cast.h"
#include "normal/core/expression/Column.h"
#include "normal/core/type/Types.h"
#include "logical/SumLogicalFunction.h"

using namespace normal::core::type;
using namespace normal::core::expression;

/**
 * SQL parse tree visitor. Converts the parse tree into a logical graph of operators.
 *
 * @param catalogues
 * @param OperatorManager
 */
Visitor::Visitor(
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<Catalogue>>> catalogues,
    std::shared_ptr<normal::core::OperatorManager> OperatorManager) :
    catalogues_(std::move(catalogues)),
    operatorManager_(std::move(OperatorManager)) {}

/**
 * The root of the parse tree. Our grammar supports multiple statements so we fidlle with it here to collate
 * them here into a vector.
 *
 * @param ctx
 * @return
 */
antlrcpp::Any Visitor::visitParse(normal::sql::NormalSQLParser::ParseContext *ctx) {

  auto sqlStatements = std::make_shared<std::vector<std::shared_ptr<std::vector<std::shared_ptr<LogicalOperator>>>>>();

  for (const auto &sql_stmt: ctx->sql_stmt_list()) {
    auto sqlStmt = visitSql_stmt_list(sql_stmt);
    sqlStatements->emplace_back(sqlStmt);
  }

  return sqlStatements;
}

antlrcpp::Any Visitor::visitSql_stmt_list(normal::sql::NormalSQLParser::Sql_stmt_listContext *ctx) {
  auto res = NormalSQLBaseVisitor::visitSql_stmt_list(ctx);
  return res;
}

antlrcpp::Any Visitor::visitSql_stmt(normal::sql::NormalSQLParser::Sql_stmtContext *ctx) {
  auto res = NormalSQLBaseVisitor::visitSql_stmt(ctx);
  return res;
}

antlrcpp::Any Visitor::visitFactored_select_stmt(normal::sql::NormalSQLParser::Factored_select_stmtContext *ctx) {
  auto res = NormalSQLBaseVisitor::visitFactored_select_stmt(ctx);
  return res;
}

/**
 * Select_core is the root of the select statement.
 *
 * @param ctx
 * @return
 */
antlrcpp::Any Visitor::visitSelect_core(normal::sql::NormalSQLParser::Select_coreContext *ctx) {

  auto nodes = std::make_shared<std::vector<std::shared_ptr<LogicalOperator>>>();

  auto collate = std::make_shared<CollateLogicalOperator>();
  collate->name = "collate";
  nodes->emplace_back(collate);

  auto scanNodes = std::make_shared<std::vector<std::shared_ptr<LogicalOperator>>>();

  for (const auto &tableOrSubquery: ctx->table_or_subquery()) {
    antlrcpp::Any tableOrSubquery_Result = visitTable_or_subquery(tableOrSubquery);
    std::shared_ptr<LogicalOperator> node = tableOrSubquery_Result.as<std::shared_ptr<LogicalOperator>>();
    nodes->emplace_back(tableOrSubquery_Result);
    scanNodes->emplace_back(tableOrSubquery_Result);
  }

  bool simpleScan = false;
  bool aggregate = false;

  auto aggregateNodes = std::make_shared<std::vector<std::shared_ptr<LogicalOperator>>>();

  for (const auto &resultColumn: ctx->result_column()) {
    auto resultColumn_Result = visitResult_column(resultColumn);
    if (resultColumn_Result.is<std::string>()) {
      // FIXME: Need to project or push down possibly, we'll just connect scan to collate for now
      simpleScan = true;
    } else if (resultColumn_Result.is<std::shared_ptr<AggregateLogicalFunction>>()) {
      aggregate = true;
      // FIXME: Only supporting 1 agg function at mo

      auto aggregateFunction = resultColumn_Result.as<std::shared_ptr<AggregateLogicalFunction>>();
      auto aggregateFunctions = {aggregateFunction};
      auto node = std::make_shared<AggregateLogicalOperator>(aggregateFunctions);
      node->name = "agg";
      nodes->push_back(node);
      aggregateNodes->push_back(node);
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

  // Aggregate query
  if(aggregate){
    for(const auto &scanNode: *scanNodes){
      for(const auto &aggregateNode: *aggregateNodes){
        scanNode->consumer = aggregateNode;
      }
    }
    for(const auto &aggregateNode: *aggregateNodes){
      aggregateNode->consumer = collate;
    }
  }

  return nodes;
}

/**
 * Table_or_subquery is the portion of the quert after FROM. Mostly deals with establishing scan operators.
 *
 * @param ctx
 * @return
 */
antlrcpp::Any Visitor::visitTable_or_subquery(normal::sql::NormalSQLParser::Table_or_subqueryContext *ctx) {

  auto catalogueName = ctx->database_name()->any_name()->IDENTIFIER()->getText();
  auto tableName = ctx->table_name()->any_name()->IDENTIFIER()->getText();

  auto catalogue = this->catalogues_->find(catalogueName);
  if (catalogue == this->catalogues_->end())
    throw std::runtime_error("Catalogue '" + catalogueName + "' not found.");

  auto catalogueEntry = catalogue->second->getEntry(tableName);

  auto scanOp = catalogueEntry->toLogicalOperator();

  return std::static_pointer_cast<LogicalOperator>(scanOp);
}

/**
 * An expression can be any element in a select list essentially, including aggregate operations, projections, column
 * functions like cast etc.
 *
 * @param ctx
 * @return
 */
antlrcpp::Any Visitor::visitExpr(normal::sql::NormalSQLParser::ExprContext *ctx) {
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
    auto typedFunction = function.as<std::shared_ptr<AggregateLogicalFunction>>();
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
antlrcpp::Any Visitor::visitFunction_name(normal::sql::NormalSQLParser::Function_nameContext *ctx) {
  if (ctx->any_name()->IDENTIFIER()->toString() == "sum") {
    auto typedFunction = std::make_shared<SumLogicalFunction>();
    return std::static_pointer_cast<AggregateLogicalFunction>(typedFunction);
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
antlrcpp::Any Visitor::visitColumn_name(normal::sql::NormalSQLParser::Column_nameContext *Context) {
  return col(Context->any_name()->IDENTIFIER()->toString());
}

/**
 * A type name, used in cast operations
 *
 * @param Context
 * @return
 */
antlrcpp::Any Visitor::visitType_name(normal::sql::NormalSQLParser::Type_nameContext *Context) {
  return typed_visitType_name(Context);
}

std::shared_ptr<normal::core::type::Type> Visitor::typed_visitType_name(normal::sql::NormalSQLParser::Type_nameContext *Context) {
  std::string typeName = Context->name(0)->any_name()->IDENTIFIER()->toString();
  return Types::fromStringType(const_cast<std::string &&>(typeName));
}

/**
 * A result column is a single element in a select list
 *
 * @param ctx
 * @return
 */
antlrcpp::Any Visitor::visitResult_column(normal::sql::NormalSQLParser::Result_columnContext *ctx) {
  if (ctx->STAR()) {
    return ctx->STAR()->toString();
  } else if (ctx->expr()) {
    return visitExpr(ctx->expr());
  } else {
    throw std::runtime_error("Cannot parse result column " + ctx->getText());
  }
}

