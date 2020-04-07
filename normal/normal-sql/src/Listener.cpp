//
// Created by matt on 26/3/20.
//

#include "Listener.h"

#include <utility>
#include <normal/pushdown/FileScan.h>
#include <connector/local-fs/LocalFileSystemCatalogueEntry.h>
#include <normal/pushdown/Collate.h>
#include "Globals.h"
#include "logical/ScanNode.h"
#include "ast/CollateNode.h"
#include "ast/AggregateNode.h"
#include "ast/AggregateExpression.h"
#include "normal/core/expression/Cast.h"
#include "normal/core/expression/Column.h"
#include "normal/core/type/Types.h"
#include "ast/SumASTFunction.h"

using namespace normal::core::type;
using namespace normal::core::expression;

Listener::Listener(
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<Catalogue>>> catalogues,
    std::shared_ptr<normal::core::OperatorManager> OperatorManager) :
    catalogues_(std::move(catalogues)),
    operatorManager_(std::move(OperatorManager)) {}

antlrcpp::Any Listener::visitParse(normal::sql::NormalSQLParser::ParseContext *ctx) {

  auto sqlStatements = std::make_shared<std::vector<std::shared_ptr<std::vector<std::shared_ptr<ASTNode>>>>>();

  for (const auto &sql_stmt: ctx->sql_stmt_list()) {
    auto sqlStmt = visitSql_stmt_list(sql_stmt);
    sqlStatements->push_back(sqlStmt);
  }

  return sqlStatements;
}

antlrcpp::Any Listener::visitSql_stmt_list(normal::sql::NormalSQLParser::Sql_stmt_listContext *ctx) {
  auto res = NormalSQLBaseVisitor::visitSql_stmt_list(ctx);
  return res;
}

antlrcpp::Any Listener::visitSql_stmt(normal::sql::NormalSQLParser::Sql_stmtContext *ctx) {
  auto res = NormalSQLBaseVisitor::visitSql_stmt(ctx);
  return res;
}

antlrcpp::Any Listener::visitFactored_select_stmt(normal::sql::NormalSQLParser::Factored_select_stmtContext *ctx) {
  auto res = NormalSQLBaseVisitor::visitFactored_select_stmt(ctx);
  return res;
}

antlrcpp::Any Listener::visitSelect_core(normal::sql::NormalSQLParser::Select_coreContext *ctx) {

  auto nodes = std::make_shared<std::vector<std::shared_ptr<ASTNode>>>();

  for (const auto &resultColumn: ctx->result_column()) {
    auto resultColumn_Result = visitResult_column(resultColumn);
    if (resultColumn_Result.is<std::string>()) {
      // FIXME: Need to push down possibly
    } else if (resultColumn_Result.is<std::shared_ptr<AggregateFunction>>()) {

      // FIXME: Only supporting 1 agg function at mo

      auto aggregateFunction = resultColumn_Result.as<std::shared_ptr<AggregateFunction>>();
      auto aggregateFunctions = {aggregateFunction};
      auto node = std::make_shared<AggregateNode>(aggregateFunctions);
      node->name = "agg";
      nodes->push_back(node);
    }
  }

  for (const auto &tableOrSubquery: ctx->table_or_subquery()) {
    antlrcpp::Any tableOrSubquery_Result = visitTable_or_subquery(tableOrSubquery);
    std::shared_ptr<ASTNode> node = tableOrSubquery_Result.as<std::shared_ptr<ASTNode>>();
    nodes->emplace_back(tableOrSubquery_Result);
  }

  auto collate = std::make_shared<CollateNode>();
  collate->name = "collate";
  nodes->emplace_back(collate);

  return nodes;
}

antlrcpp::Any Listener::visitTable_or_subquery(normal::sql::NormalSQLParser::Table_or_subqueryContext *ctx) {

  auto catalogueName = ctx->database_name()->any_name()->IDENTIFIER()->getText();
  auto tableName = ctx->table_name()->any_name()->IDENTIFIER()->getText();

  auto catalogue = this->catalogues_->find(catalogueName);
  if (catalogue == this->catalogues_->end())
    throw std::runtime_error("Catalogue '" + catalogueName + "' not found.");

  auto catalogueEntry = catalogue->second->getEntry(tableName);

  auto scanOp = catalogueEntry->toLogicalOperator();

  return std::static_pointer_cast<ASTNode>(scanOp);
}

antlrcpp::Any Listener::visitExpr(normal::sql::NormalSQLParser::ExprContext *ctx) {
  if (ctx->K_CAST()) {
    return cast(visitColumn_name(ctx->column_name()), visitType_name(ctx->type_name()));
  } else if (ctx->column_name()) {
    return visitColumn_name(ctx->column_name());
  } else if (ctx->function_name()) {
    auto function = visitFunction_name(ctx->function_name());
    auto typedFunction = function.as<std::shared_ptr<AggregateFunction>>();
    typedFunction->expression(visitExpr(ctx->expr(0)));
    return function;
  } else {
    throw std::runtime_error("Cannot parse expression " + ctx->getText());
  }
}

antlrcpp::Any Listener::visitFunction_name(normal::sql::NormalSQLParser::Function_nameContext *ctx) {
  if (ctx->any_name()->IDENTIFIER()->toString() == "sum") {
    auto typedFunction = std::make_shared<SumASTFunction>();
    return std::static_pointer_cast<AggregateFunction>(typedFunction);
  }
  else{
    throw std::runtime_error("Unrecognized function " + ctx->any_name()->IDENTIFIER()->toString());
  }
}

antlrcpp::Any Listener::visitColumn_name(normal::sql::NormalSQLParser::Column_nameContext *Context) {
  return col(Context->any_name()->IDENTIFIER()->toString());
}

antlrcpp::Any Listener::visitType_name(normal::sql::NormalSQLParser::Type_nameContext *Context) {
  return Types::fromStringType(Context->toString());
}

antlrcpp::Any Listener::visitResult_column(normal::sql::NormalSQLParser::Result_columnContext *ctx) {
  if (ctx->STAR()) {
    return ctx->STAR()->toString();
  } else if (ctx->expr()) {
    return visitExpr(ctx->expr());
  } else {
    throw std::runtime_error("Cannot parse result column " + ctx->getText());
  }
}

//void Listener::enterSelect_core(normal::sql::NormalSQLParser::Select_coreContext *Context) {

//  parseTableOrSubquery(Context->table_or_subquery());

//  auto scan = std::make_shared<ScanNode>();
//  auto collate = std::make_shared<CollateNode>();
//
//  collate->name = "collate";
//
//  symbolTable.table.insert(std::pair(0, scan));
//  symbolTable.table.insert(std::pair(1, collate));
//
//  Context->table_or_subquery();
//
//  // Scan table source
//  auto catalogueName = Context->table_or_subquery(0)->database_name()->any_name()->IDENTIFIER()->getText();
//  auto tableName = Context->table_or_subquery(0)->table_name()->any_name()->IDENTIFIER()->getText();
//
//  auto catalogue = this->catalogues_->find(catalogueName);
//  if (catalogue == this->catalogues_->end())
//    throw std::runtime_error("Catalogue '" + catalogueName + "' not found.");
//
//  auto localFS = std::static_pointer_cast<LocalFileSystemCatalogueEntry>(catalogue->second->getEntry(tableName));
//
//  scan->name = catalogueName + "." + tableName;
//  scan->tableName = localFS;
//
//  // Aggregate functions
//  for(const auto & resultColumn: Context->result_column()){
//    if(resultColumn->STAR()){
//      // FIXME: Should set up project * but just do nothing for now
//      scan->consumer = collate;
//    }
//    else if(resultColumn->expr()->function_name()){
//      // Aggregate
//      auto functionNameContext = Context->result_column(0)->expr()->function_name();
//
//      if(functionNameContext != nullptr){
//        auto functionName = functionNameContext->any_name()->IDENTIFIER()->toString();
//        std::transform(functionName.begin(), functionName.end(), functionName.begin(), ::tolower);
//        if(functionName =="sum"){
//
//          auto aggregate = std::make_shared<AggregateNode>();
//
//          auto aggregateFunction = std::make_shared<AggregateFunction>("sum");
//
//          auto columnNameExpressionContext = Context->result_column(0)->expr()->expr(0)->column_name();
//          auto columnName = columnNameExpressionContext->any_name()->IDENTIFIER()->getText();
//
//          auto aggregateExpression = std::make_shared<AggregateExpression>(columnName);
//          aggregateFunction->expression = aggregateExpression;
//
//          aggregate->functions.emplace_back(aggregateFunction);
//
//          aggregate->name = "sum(" + columnName + ")";
//
//          symbolTable.table.insert(std::pair(2, aggregate));
//
//          scan->consumer = aggregate;
//          aggregate->consumer = collate;
//        }
//      }
//      else{
//        scan->consumer = collate;
//      }
//    }
//    else{
//      throw std::runtime_error("Unrecognized result columns expression " + resultColumn->getText());
//    }
//  }

//  void Listener::enterTable_or_subquery(normal::sql::NormalSQLParser::Table_or_subqueryContext *Context) {
//
//    auto scan = std::make_shared<ScanNode>();
//
//    auto catalogueName = Context->database_name()->any_name()->IDENTIFIER()->getText();
//    auto tableName = Context->table_name()->any_name()->IDENTIFIER()->getText();
//
//    auto catalogue = this->catalogues_->find(catalogueName);
//    if (catalogue == this->catalogues_->end())
//      throw std::runtime_error("Catalogue '" + catalogueName + "' not found.");
//
//    auto catalogueEntry =
//        std::static_pointer_cast<LocalFileSystemCatalogueEntry>(catalogue->second->getEntry(tableName));
//
//    scan->name = catalogueName + "." + tableName;
//    scan->tableName = catalogueEntry;
//
//    symbolTable.table.insert(std::pair(scan->name, scan));
//  }

//void Listener::enterResult_column(normal::sql::NormalSQLParser::Result_columnContext *Context) {
//  if(Context->STAR()) {
//    // FIXME: Should set up project * but just connect collate to last producing operator
////      scan->consumer = collate;
//  }
//  else if(Context->expr()->function_name()) {
//    // Aggregate
//    auto functionNameContext = Context->result_column(0)->expr()->function_name();
//
//    if (functionNameContext != nullptr) {
//      auto functionName = functionNameContext->any_name()->IDENTIFIER()->toString();
//      std::transform(functionName.begin(), functionName.end(), functionName.begin(), ::tolower);
//      if (functionName == "sum") {
//
//        auto aggregate = std::make_shared<AggregateNode>();
//
//        auto aggregateFunction = std::make_shared<AggregateFunction>("sum");
//
//        auto columnNameExpressionContext = Context->result_column(0)->expr()->expr(0)->column_name();
//        auto columnName = columnNameExpressionContext->any_name()->IDENTIFIER()->getText();
//
//        auto aggregateExpression = std::make_shared<AggregateExpression>(columnName);
//        aggregateFunction->expression = aggregateExpression;
//
//        aggregate->functions.emplace_back(aggregateFunction);
//
//        aggregate->name = "sum(" + columnName + ")";
//
//        symbolTable.table.insert(std::pair(2, aggregate));
//
//        scan->consumer = aggregate;
//        aggregate->consumer = collate;
//      }
//    }
//  }
//
//}



//  void Listener::enterFunction_name(normal::sql::NormalSQLParser::Function_nameContext *Context) {
//
//    auto functionName = Context->any_name()->IDENTIFIER()->toString();
//    std::transform(functionName.begin(), functionName.end(), functionName.begin(), ::tolower);
//
//    if (functionName == "sum") {
//
//      auto aggregate = std::make_shared<AggregateNode>();
//      symbolTable.table.insert(std::pair(2, aggregate));
//
//      auto sumFunction = std::make_shared<AggregateFunction>("sum");
//
//      auto columnNameExpressionContext = Context->result_column(0)->expr()->expr(0)->column_name();
//      auto columnName = columnNameExpressionContext->any_name()->IDENTIFIER()->getText();
//
//      auto aggregateExpression = std::make_shared<AggregateExpression>(columnName);
//      aggregateFunction->expression = aggregateExpression;
//
//      aggregate->functions.emplace_back(aggregateFunction);
//
//      aggregate->name = "sum(" + columnName + ")";
//
//
//
//      scan->consumer = aggregate;
//      aggregate->consumer = collate;
//    }
//  }


