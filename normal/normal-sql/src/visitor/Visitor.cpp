//
// Created by matt on 26/3/20.
//

#include "Visitor.h"

#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/expression/gandiva/Multiply.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/GreaterThan.h>
#include <normal/expression/gandiva/LessThanOrEqualTo.h>
#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
#include <normal/expression/gandiva/EqualTo.h>

#include <normal/core/type/Types.h>
#include <normal/plan/LogicalPlan.h>
#include <normal/expression/gandiva/Or.h>
#include <normal/expression/gandiva/Literal.h>
#include "normal/plan/operator_/ProjectLogicalOperator.h"
#include "normal/plan/operator_/CollateLogicalOperator.h"
#include "normal/plan/operator_/AggregateLogicalOperator.h"
#include "normal/plan/function/SumLogicalFunction.h"

using namespace normal::core::type;
using namespace normal::expression;
using namespace normal::expression::gandiva;

/**
 * SQL parse tree visitor. Converts the parse tree into a logical graph of operators.
 *
 * @param catalogues
 * @param OperatorManager
 */
normal::sql::visitor::Visitor::Visitor(
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<normal::connector::Catalogue>>> catalogues,
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

  auto allQueryPlans = std::make_shared<std::vector<std::shared_ptr<plan::LogicalPlan>>>();

  for (const auto &sql_stmt: ctx->sql_stmt_list()) {
    auto untypedQueryPlans = visitSql_stmt_list(sql_stmt);
    auto queryPlans = untypedQueryPlans.as<std::shared_ptr<std::vector<std::shared_ptr<plan::LogicalPlan>>>>();
    allQueryPlans->insert(allQueryPlans->end(), queryPlans->begin(), queryPlans->end());
  }

  return allQueryPlans;
}

antlrcpp::Any normal::sql::visitor::Visitor::visitSql_stmt_list(normal::sql::NormalSQLParser::Sql_stmt_listContext *ctx) {
  auto queryPlans = std::make_shared<std::vector<std::shared_ptr<plan::LogicalPlan>>>();

  for (const auto &sql_stmt: ctx->sql_stmt()) {
    auto res = visitSql_stmt(sql_stmt);
    auto logicalOperators = res.as<std::shared_ptr<std::vector<std::shared_ptr<normal::plan::operator_::LogicalOperator>>>>();
    auto queryPlan = std::make_shared<plan::LogicalPlan>(logicalOperators);
    queryPlans->push_back(queryPlan);
  }

  return queryPlans;
}

antlrcpp::Any normal::sql::visitor::Visitor::visitSql_stmt(normal::sql::NormalSQLParser::Sql_stmtContext *ctx) {
  auto res = NormalSQLBaseVisitor::visitSql_stmt(ctx);
  return res;
}

antlrcpp::Any normal::sql::visitor::Visitor::visitSelect_stmt(normal::sql::NormalSQLParser::Select_stmtContext *ctx) {
  auto res = NormalSQLBaseVisitor::visitSelect_stmt(ctx);
  return res;
}

/**
 * Select_core is the root of the select statement.
 *
 * @param ctx
 * @return
 */
antlrcpp::Any normal::sql::visitor::Visitor::visitSelect_core(normal::sql::NormalSQLParser::Select_coreContext *ctx) {

  auto nodes = std::make_shared<std::vector<std::shared_ptr<normal::plan::operator_::LogicalOperator>>>();

  auto collate = std::make_shared<normal::plan::operator_::CollateLogicalOperator>();
  collate->setName("collate");
  nodes->emplace_back(collate);

  // visit from_clause
  auto res_from_clause = visitFrom_clause(ctx->from_clause());
  auto scanNodes = res_from_clause.as<std::shared_ptr<std::vector<std::shared_ptr<normal::plan::operator_::LogicalOperator>>>>();

  bool simpleScan = false;
  bool project = false;
  bool aggregate = false;
  bool join = ((*scanNodes).size() > 1) ? true : false;

  // visit where_clause (conjunctive exprs currently)
  auto andExpr = std::make_shared<std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>>();
  auto res_where_clause = visitWhere_clause(ctx->where_clause());
  if (res_where_clause.is<std::shared_ptr<normal::expression::gandiva::Expression>>()) {
    andExpr->emplace_back(res_where_clause);
  } else if (res_where_clause.is<std::shared_ptr<std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>>>()) {
    andExpr = res_where_clause;
  }

  // extract filters and join predicates respectively
  for (auto expr: *andExpr) {

  }








  auto aggregateFunctions = std::make_shared<std::vector<std::shared_ptr<normal::plan::function::AggregateLogicalFunction>>>();
  auto projectExpressions = std::make_shared<std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>>();

  for (const auto &resultColumn: ctx->result_column()) {
    auto resultColumn_Result = visitResult_column(resultColumn);
    if (resultColumn_Result.is<std::string>()) {
      simpleScan = true;
    } else if (resultColumn_Result.is<std::shared_ptr<normal::plan::function::AggregateLogicalFunction>>()) {
      aggregate = true;
	    auto aggregateFunction = resultColumn_Result.as<std::shared_ptr<normal::plan::function::AggregateLogicalFunction>>();
	    aggregateFunctions->push_back(aggregateFunction);
    } else if (resultColumn_Result.is<std::shared_ptr<normal::expression::gandiva::Expression>>()) {
      project = true;
      auto projectExpression = resultColumn_Result.as<std::shared_ptr<normal::expression::gandiva::Expression>>();
	    projectExpressions->push_back(projectExpression);
    }
    else{
      throw std::runtime_error("Not yet implemented");
    }
  }

  // Simple scan
  if(simpleScan){
    for(const auto &scanNode: *scanNodes){
      scanNode->setConsumer(collate);
    }
  }

  // Projection
  if(project){

	auto projectNode = std::make_shared<normal::plan::operator_::ProjectLogicalOperator>(*projectExpressions);
	projectNode->setName("proj");
	nodes->push_back(projectNode);

    for(const auto &scanNode: *scanNodes){
	  scanNode->setConsumer(projectNode);
	}

    projectNode->setConsumer(collate);
  }

  // Aggregate query
  if(aggregate){
    auto aggregateNode = std::make_shared<normal::plan::operator_::AggregateLogicalOperator>(*aggregateFunctions);
    aggregateNode->setName( "agg");
    nodes->push_back(aggregateNode);

	  for(const auto &scanNode: *scanNodes){
	    scanNode->setConsumer(aggregateNode);
	  }

	  aggregateNode->setConsumer(collate);
  }

  return nodes;
}

antlrcpp::Any normal::sql::visitor::Visitor::visitFrom_clause(normal::sql::NormalSQLParser::From_clauseContext *ctx) {
  auto scanNodes = std::make_shared<std::vector<std::shared_ptr<normal::plan::operator_::LogicalOperator>>>();

  for (const auto &tableOrSubquery: ctx->table_or_subquery()) {
    antlrcpp::Any tableOrSubquery_Result = visitTable_or_subquery(tableOrSubquery);
    std::shared_ptr<normal::plan::operator_::LogicalOperator> node = tableOrSubquery_Result.as<std::shared_ptr<normal::plan::operator_::LogicalOperator>>();
    scanNodes->emplace_back(tableOrSubquery_Result);
  }

  return scanNodes;
}

antlrcpp::Any normal::sql::visitor::Visitor::visitWhere_clause(normal::sql::NormalSQLParser::Where_clauseContext *ctx) {
  auto res = visit(ctx->expr());
  return res;
}

antlrcpp::Any
normal::sql::visitor::Visitor::visitGroupBy_clause(normal::sql::NormalSQLParser::GroupBy_clauseContext *ctx) {
  return NormalSQLBaseVisitor::visitGroupBy_clause(ctx);
}

/**
 * Table_or_subquery is the portion of the query after FROM. Mostly deals with establishing scan operators.
 *
 * @param ctx
 * @return
 */
antlrcpp::Any normal::sql::visitor::Visitor::visitTable_or_subquery(normal::sql::NormalSQLParser::Table_or_subqueryContext *ctx) {

  auto tableName = ctx->table_name()->any_name()->IDENTIFIER()->getText();

  std::shared_ptr<connector::Catalogue> catalogue;
  if(ctx->database_name()){
    auto catalogueName = ctx->database_name()->any_name()->IDENTIFIER()->getText();
    auto catalogueIterator = this->catalogues_->find(catalogueName);
    if (catalogueIterator == this->catalogues_->end())
      throw std::runtime_error("Catalogue '" + catalogueName + "' does not exist.");
    catalogue = catalogueIterator->second;
  }
  else{
    // No catalogue specified, use default catalogue
    // Let's currently use "s3_select" as default
    std::string catalogueName = "s3_select";
    auto catalogueIterator = this->catalogues_->find(catalogueName);
    if (catalogueIterator == this->catalogues_->end())
      throw std::runtime_error("Catalogue '" + catalogueName + "' does not exist.");
    catalogue = catalogueIterator->second;
  }

  auto catalogueEntryExpected = catalogue->entry(tableName);
  if(!catalogueEntryExpected) {
    // FIXME: Propagate errors properly
	throw std::runtime_error("Table '" + tableName + "' does not exist in catalogue '" + catalogue->getName() + "'");
  }
  else{
	auto scanOp = catalogueEntryExpected.value()->toLogicalOperator();
	return std::static_pointer_cast<normal::plan::operator_::LogicalOperator>(scanOp);
  }
}

antlrcpp::Any normal::sql::visitor::Visitor::visitExpr_literal(normal::sql::NormalSQLParser::Expr_literalContext *ctx) {
  auto value = visit(ctx->literal_value());
  if (value.is<int>()) {
    return lit<::arrow::Int32Type>(value);
  } else if (value.is<float>()) {
    return lit<::arrow::FloatType>(value);
  } else if (value.is<std::string>()) {
    return lit<::arrow::StringType>(value);
  } else {
    throw std::runtime_error("Unimplemented type");
  }
}

antlrcpp::Any normal::sql::visitor::Visitor::visitExpr_cast(normal::sql::NormalSQLParser::Expr_castContext *ctx) {
  std::shared_ptr<normal::expression::gandiva::Expression> expression = visit(ctx->expr());
  auto typeNameCtx = ctx->type_name();
  auto type = typed_visitType_name(typeNameCtx);
  return cast(expression, type);
}

antlrcpp::Any normal::sql::visitor::Visitor::visitExpr_column(normal::sql::NormalSQLParser::Expr_columnContext *ctx) {
  return visitColumn_name(ctx->column_name());
}

antlrcpp::Any
normal::sql::visitor::Visitor::visitExpr_function(normal::sql::NormalSQLParser::Expr_functionContext *ctx) {
  auto function = visitFunction_name(ctx->function_name());
  auto typedFunction = function.as<std::shared_ptr<normal::plan::function::AggregateLogicalFunction>>();
  typedFunction->expression(visit(ctx->expr(0)));
  return function;
}

antlrcpp::Any
normal::sql::visitor::Visitor::visitExpr_mul_div_mod(normal::sql::NormalSQLParser::Expr_mul_div_modContext *ctx) {
  if(ctx->STAR()){
    std::shared_ptr<normal::expression::gandiva::Expression> leftExpression = visit(ctx->expr(0));
    std::shared_ptr<normal::expression::gandiva::Expression> rightExpression = visit(ctx->expr(1));
    return times(leftExpression, rightExpression);
  } else {
    throw std::runtime_error("\"/\" and \"%\" are not implemented");
  }
}

antlrcpp::Any normal::sql::visitor::Visitor::visitExpr_comp(normal::sql::NormalSQLParser::Expr_compContext *ctx) {
  auto leftExprCtx = ctx->expr(0);
  auto rightExprCtx = ctx->expr(1);
  if(ctx->LT()) {
    std::shared_ptr<normal::expression::gandiva::Expression> leftExpression = visit(leftExprCtx);
    std::shared_ptr<normal::expression::gandiva::Expression> rightExpression = visit(rightExprCtx);
    return lt(leftExpression, rightExpression);
  }
  else if(ctx->LT_EQ()) {
    std::shared_ptr<normal::expression::gandiva::Expression> leftExpression = visit(leftExprCtx);
    std::shared_ptr<normal::expression::gandiva::Expression> rightExpression = visit(rightExprCtx);
    return lte(leftExpression, rightExpression);
  }
  else if(ctx->GT()) {
    std::shared_ptr<normal::expression::gandiva::Expression> leftExpression = visit(leftExprCtx);
    std::shared_ptr<normal::expression::gandiva::Expression> rightExpression = visit(rightExprCtx);
    return gt(leftExpression, rightExpression);
  }
  else {
    std::shared_ptr<normal::expression::gandiva::Expression> leftExpression = visit(leftExprCtx);
    std::shared_ptr<normal::expression::gandiva::Expression> rightExpression = visit(rightExprCtx);
    return gte(leftExpression, rightExpression);
  }
}

antlrcpp::Any normal::sql::visitor::Visitor::visitExpr_eq(normal::sql::NormalSQLParser::Expr_eqContext *ctx) {
  auto leftExprCtx = ctx->expr(0);
  auto rightExprCtx = ctx->expr(1);
  if (ctx->ASSIGN() || ctx->EQ()) {
    std::shared_ptr<normal::expression::gandiva::Expression> leftExpression = visit(leftExprCtx);
    std::shared_ptr<normal::expression::gandiva::Expression> rightExpression = visit(rightExprCtx);
    return eq(leftExpression, rightExpression);
  } else {
    throw std::runtime_error("\"!=\" and \"<>\" are not implemented");
  }
}

antlrcpp::Any normal::sql::visitor::Visitor::visitExpr_between(normal::sql::NormalSQLParser::Expr_betweenContext *ctx) {
  auto andExpr = std::make_shared<std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>>();
  std::shared_ptr<normal::expression::gandiva::Expression> centerExpression = visit(ctx->expr(0));
  std::shared_ptr<normal::expression::gandiva::Expression> leftExpression = visit(ctx->expr(1));
  std::shared_ptr<normal::expression::gandiva::Expression> rightExpression = visit(ctx->expr(2));
  andExpr->emplace_back(gte(centerExpression, leftExpression));
  andExpr->emplace_back(lte(centerExpression, rightExpression));
  return andExpr;
}

/**
 * visit "and": return a vector of expressions
 * visit "or": return an Or_Expression
 * because ssb only involving "or" of 2, but "and" of more than 2
 */
antlrcpp::Any normal::sql::visitor::Visitor::visitExpr_and(normal::sql::NormalSQLParser::Expr_andContext *ctx) {
  auto andExpr = std::make_shared<std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>>();
  auto res_left = visit(ctx->expr(0));
  if (res_left.is<std::shared_ptr<normal::expression::gandiva::Expression>>()) {
    auto leftExpression = res_left.as<std::shared_ptr<normal::expression::gandiva::Expression>>();
    andExpr->emplace_back(leftExpression);
  } else {
    auto leftExpressions = res_left.as<std::shared_ptr<std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>>>();
    andExpr->insert(andExpr->end(), leftExpressions->begin(), leftExpressions->end());
  }
  auto res_right = visit(ctx->expr(1));
  if (res_right.is<std::shared_ptr<normal::expression::gandiva::Expression>>()) {
    auto rightExpression = res_right.as<std::shared_ptr<normal::expression::gandiva::Expression>>();
    andExpr->emplace_back(rightExpression);
  } else {
    auto rightExpressions = res_right.as<std::shared_ptr<std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>>>();
    andExpr->insert(andExpr->end(), rightExpressions->begin(), rightExpressions->end());
  }
  return andExpr;
}

antlrcpp::Any normal::sql::visitor::Visitor::visitExpr_or(normal::sql::NormalSQLParser::Expr_orContext *ctx) {
  std::shared_ptr<normal::expression::gandiva::Expression> leftExpression = visit(ctx->expr(0));
  std::shared_ptr<normal::expression::gandiva::Expression> rightExpression = visit(ctx->expr(1));
  return or_(leftExpression, rightExpression);
}

antlrcpp::Any normal::sql::visitor::Visitor::visitExpr_parens(normal::sql::NormalSQLParser::Expr_parensContext *ctx) {
  return visit(ctx->expr());
}

antlrcpp::Any normal::sql::visitor::Visitor::visitLiteral_value_numeric(
        normal::sql::NormalSQLParser::Literal_value_numericContext *ctx) {
  auto numeric_str = ctx->getText();
  auto idx = numeric_str.find(".");
  if (idx == std::string::npos) {
    int value = std::stoi(numeric_str);
    return value;
  } else {
    float value = std::stof(numeric_str);
    return value;
  }
}

antlrcpp::Any normal::sql::visitor::Visitor::visitLiteral_value_string(
        normal::sql::NormalSQLParser::Literal_value_stringContext *ctx) {
  auto raw_str = ctx->getText();
  std::string str = raw_str.substr(1, raw_str.size() - 2);
  return str;
}

/**
 * Functions are aggregates or column functions.
 *
 * @param ctx
 * @return
 */
antlrcpp::Any normal::sql::visitor::Visitor::visitFunction_name(normal::sql::NormalSQLParser::Function_nameContext *ctx) {
  if (ctx->any_name()->IDENTIFIER()->toString() == "sum") {
    auto typedFunction = std::make_shared<normal::plan::function::SumLogicalFunction>();
    return std::static_pointer_cast<normal::plan::function::AggregateLogicalFunction>(typedFunction);
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
    return visit(ctx->expr());
  } else {
    throw std::runtime_error("Cannot parse result column " + ctx->getText());
  }
}

