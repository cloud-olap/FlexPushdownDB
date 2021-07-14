
// Generated from /Users/yyf/Desktop/pushdownDB/normal/normal-sql/grammar/NormalSQL.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"
#include "NormalSQLParser.h"


namespace normal::sql {

/**
 * This interface defines an abstract listener for a parse tree produced by NormalSQLParser.
 */
class  NormalSQLListener : public antlr4::tree::ParseTreeListener {
public:

  virtual void enterParse(NormalSQLParser::ParseContext *ctx) = 0;
  virtual void exitParse(NormalSQLParser::ParseContext *ctx) = 0;

  virtual void enterError(NormalSQLParser::ErrorContext *ctx) = 0;
  virtual void exitError(NormalSQLParser::ErrorContext *ctx) = 0;

  virtual void enterSql_stmt_list(NormalSQLParser::Sql_stmt_listContext *ctx) = 0;
  virtual void exitSql_stmt_list(NormalSQLParser::Sql_stmt_listContext *ctx) = 0;

  virtual void enterSql_stmt(NormalSQLParser::Sql_stmtContext *ctx) = 0;
  virtual void exitSql_stmt(NormalSQLParser::Sql_stmtContext *ctx) = 0;

  virtual void enterSelect_stmt(NormalSQLParser::Select_stmtContext *ctx) = 0;
  virtual void exitSelect_stmt(NormalSQLParser::Select_stmtContext *ctx) = 0;

  virtual void enterSelect_core(NormalSQLParser::Select_coreContext *ctx) = 0;
  virtual void exitSelect_core(NormalSQLParser::Select_coreContext *ctx) = 0;

  virtual void enterFrom_clause(NormalSQLParser::From_clauseContext *ctx) = 0;
  virtual void exitFrom_clause(NormalSQLParser::From_clauseContext *ctx) = 0;

  virtual void enterWhere_clause(NormalSQLParser::Where_clauseContext *ctx) = 0;
  virtual void exitWhere_clause(NormalSQLParser::Where_clauseContext *ctx) = 0;

  virtual void enterGroupBy_clause(NormalSQLParser::GroupBy_clauseContext *ctx) = 0;
  virtual void exitGroupBy_clause(NormalSQLParser::GroupBy_clauseContext *ctx) = 0;

  virtual void enterType_name(NormalSQLParser::Type_nameContext *ctx) = 0;
  virtual void exitType_name(NormalSQLParser::Type_nameContext *ctx) = 0;

  virtual void enterExpr_comp(NormalSQLParser::Expr_compContext *ctx) = 0;
  virtual void exitExpr_comp(NormalSQLParser::Expr_compContext *ctx) = 0;

  virtual void enterExpr_case(NormalSQLParser::Expr_caseContext *ctx) = 0;
  virtual void exitExpr_case(NormalSQLParser::Expr_caseContext *ctx) = 0;

  virtual void enterExpr_raise_function(NormalSQLParser::Expr_raise_functionContext *ctx) = 0;
  virtual void exitExpr_raise_function(NormalSQLParser::Expr_raise_functionContext *ctx) = 0;

  virtual void enterExpr_parens(NormalSQLParser::Expr_parensContext *ctx) = 0;
  virtual void exitExpr_parens(NormalSQLParser::Expr_parensContext *ctx) = 0;

  virtual void enterExpr_column(NormalSQLParser::Expr_columnContext *ctx) = 0;
  virtual void exitExpr_column(NormalSQLParser::Expr_columnContext *ctx) = 0;

  virtual void enterExpr_function(NormalSQLParser::Expr_functionContext *ctx) = 0;
  virtual void exitExpr_function(NormalSQLParser::Expr_functionContext *ctx) = 0;

  virtual void enterExpr_eq(NormalSQLParser::Expr_eqContext *ctx) = 0;
  virtual void exitExpr_eq(NormalSQLParser::Expr_eqContext *ctx) = 0;

  virtual void enterExpr_null(NormalSQLParser::Expr_nullContext *ctx) = 0;
  virtual void exitExpr_null(NormalSQLParser::Expr_nullContext *ctx) = 0;

  virtual void enterExpr_bind_parameter(NormalSQLParser::Expr_bind_parameterContext *ctx) = 0;
  virtual void exitExpr_bind_parameter(NormalSQLParser::Expr_bind_parameterContext *ctx) = 0;

  virtual void enterExpr_is(NormalSQLParser::Expr_isContext *ctx) = 0;
  virtual void exitExpr_is(NormalSQLParser::Expr_isContext *ctx) = 0;

  virtual void enterExpr_literal(NormalSQLParser::Expr_literalContext *ctx) = 0;
  virtual void exitExpr_literal(NormalSQLParser::Expr_literalContext *ctx) = 0;

  virtual void enterExpr_cast(NormalSQLParser::Expr_castContext *ctx) = 0;
  virtual void exitExpr_cast(NormalSQLParser::Expr_castContext *ctx) = 0;

  virtual void enterExpr_exists(NormalSQLParser::Expr_existsContext *ctx) = 0;
  virtual void exitExpr_exists(NormalSQLParser::Expr_existsContext *ctx) = 0;

  virtual void enterExpr_mul_div_mod(NormalSQLParser::Expr_mul_div_modContext *ctx) = 0;
  virtual void exitExpr_mul_div_mod(NormalSQLParser::Expr_mul_div_modContext *ctx) = 0;

  virtual void enterExpr_between(NormalSQLParser::Expr_betweenContext *ctx) = 0;
  virtual void exitExpr_between(NormalSQLParser::Expr_betweenContext *ctx) = 0;

  virtual void enterExpr_in(NormalSQLParser::Expr_inContext *ctx) = 0;
  virtual void exitExpr_in(NormalSQLParser::Expr_inContext *ctx) = 0;

  virtual void enterExpr_bit(NormalSQLParser::Expr_bitContext *ctx) = 0;
  virtual void exitExpr_bit(NormalSQLParser::Expr_bitContext *ctx) = 0;

  virtual void enterExpr_or(NormalSQLParser::Expr_orContext *ctx) = 0;
  virtual void exitExpr_or(NormalSQLParser::Expr_orContext *ctx) = 0;

  virtual void enterExpr_collate(NormalSQLParser::Expr_collateContext *ctx) = 0;
  virtual void exitExpr_collate(NormalSQLParser::Expr_collateContext *ctx) = 0;

  virtual void enterExpr_and(NormalSQLParser::Expr_andContext *ctx) = 0;
  virtual void exitExpr_and(NormalSQLParser::Expr_andContext *ctx) = 0;

  virtual void enterExpr_add_sub(NormalSQLParser::Expr_add_subContext *ctx) = 0;
  virtual void exitExpr_add_sub(NormalSQLParser::Expr_add_subContext *ctx) = 0;

  virtual void enterExpr_unary(NormalSQLParser::Expr_unaryContext *ctx) = 0;
  virtual void exitExpr_unary(NormalSQLParser::Expr_unaryContext *ctx) = 0;

  virtual void enterExpr_str_match(NormalSQLParser::Expr_str_matchContext *ctx) = 0;
  virtual void exitExpr_str_match(NormalSQLParser::Expr_str_matchContext *ctx) = 0;

  virtual void enterRaise_function(NormalSQLParser::Raise_functionContext *ctx) = 0;
  virtual void exitRaise_function(NormalSQLParser::Raise_functionContext *ctx) = 0;

  virtual void enterIndexed_column(NormalSQLParser::Indexed_columnContext *ctx) = 0;
  virtual void exitIndexed_column(NormalSQLParser::Indexed_columnContext *ctx) = 0;

  virtual void enterWith_clause(NormalSQLParser::With_clauseContext *ctx) = 0;
  virtual void exitWith_clause(NormalSQLParser::With_clauseContext *ctx) = 0;

  virtual void enterQualified_table_name(NormalSQLParser::Qualified_table_nameContext *ctx) = 0;
  virtual void exitQualified_table_name(NormalSQLParser::Qualified_table_nameContext *ctx) = 0;

  virtual void enterOrdering_term(NormalSQLParser::Ordering_termContext *ctx) = 0;
  virtual void exitOrdering_term(NormalSQLParser::Ordering_termContext *ctx) = 0;

  virtual void enterPragma_value(NormalSQLParser::Pragma_valueContext *ctx) = 0;
  virtual void exitPragma_value(NormalSQLParser::Pragma_valueContext *ctx) = 0;

  virtual void enterCommon_table_expression(NormalSQLParser::Common_table_expressionContext *ctx) = 0;
  virtual void exitCommon_table_expression(NormalSQLParser::Common_table_expressionContext *ctx) = 0;

  virtual void enterResult_column(NormalSQLParser::Result_columnContext *ctx) = 0;
  virtual void exitResult_column(NormalSQLParser::Result_columnContext *ctx) = 0;

  virtual void enterTable_or_subquery(NormalSQLParser::Table_or_subqueryContext *ctx) = 0;
  virtual void exitTable_or_subquery(NormalSQLParser::Table_or_subqueryContext *ctx) = 0;

  virtual void enterJoin_clause(NormalSQLParser::Join_clauseContext *ctx) = 0;
  virtual void exitJoin_clause(NormalSQLParser::Join_clauseContext *ctx) = 0;

  virtual void enterJoin_operator(NormalSQLParser::Join_operatorContext *ctx) = 0;
  virtual void exitJoin_operator(NormalSQLParser::Join_operatorContext *ctx) = 0;

  virtual void enterJoin_constraint(NormalSQLParser::Join_constraintContext *ctx) = 0;
  virtual void exitJoin_constraint(NormalSQLParser::Join_constraintContext *ctx) = 0;

  virtual void enterCompound_operator(NormalSQLParser::Compound_operatorContext *ctx) = 0;
  virtual void exitCompound_operator(NormalSQLParser::Compound_operatorContext *ctx) = 0;

  virtual void enterCte_table_name(NormalSQLParser::Cte_table_nameContext *ctx) = 0;
  virtual void exitCte_table_name(NormalSQLParser::Cte_table_nameContext *ctx) = 0;

  virtual void enterSigned_number(NormalSQLParser::Signed_numberContext *ctx) = 0;
  virtual void exitSigned_number(NormalSQLParser::Signed_numberContext *ctx) = 0;

  virtual void enterLiteral_value_numeric(NormalSQLParser::Literal_value_numericContext *ctx) = 0;
  virtual void exitLiteral_value_numeric(NormalSQLParser::Literal_value_numericContext *ctx) = 0;

  virtual void enterLiteral_value_string(NormalSQLParser::Literal_value_stringContext *ctx) = 0;
  virtual void exitLiteral_value_string(NormalSQLParser::Literal_value_stringContext *ctx) = 0;

  virtual void enterLiteral_value_blob(NormalSQLParser::Literal_value_blobContext *ctx) = 0;
  virtual void exitLiteral_value_blob(NormalSQLParser::Literal_value_blobContext *ctx) = 0;

  virtual void enterLiteral_value_k_null(NormalSQLParser::Literal_value_k_nullContext *ctx) = 0;
  virtual void exitLiteral_value_k_null(NormalSQLParser::Literal_value_k_nullContext *ctx) = 0;

  virtual void enterLiteral_value_k_current_time(NormalSQLParser::Literal_value_k_current_timeContext *ctx) = 0;
  virtual void exitLiteral_value_k_current_time(NormalSQLParser::Literal_value_k_current_timeContext *ctx) = 0;

  virtual void enterLiteral_value_k_current_date(NormalSQLParser::Literal_value_k_current_dateContext *ctx) = 0;
  virtual void exitLiteral_value_k_current_date(NormalSQLParser::Literal_value_k_current_dateContext *ctx) = 0;

  virtual void enterLiteral_value_k_current_timestamp(NormalSQLParser::Literal_value_k_current_timestampContext *ctx) = 0;
  virtual void exitLiteral_value_k_current_timestamp(NormalSQLParser::Literal_value_k_current_timestampContext *ctx) = 0;

  virtual void enterUnary_operator(NormalSQLParser::Unary_operatorContext *ctx) = 0;
  virtual void exitUnary_operator(NormalSQLParser::Unary_operatorContext *ctx) = 0;

  virtual void enterError_message(NormalSQLParser::Error_messageContext *ctx) = 0;
  virtual void exitError_message(NormalSQLParser::Error_messageContext *ctx) = 0;

  virtual void enterColumn_alias(NormalSQLParser::Column_aliasContext *ctx) = 0;
  virtual void exitColumn_alias(NormalSQLParser::Column_aliasContext *ctx) = 0;

  virtual void enterKeyword(NormalSQLParser::KeywordContext *ctx) = 0;
  virtual void exitKeyword(NormalSQLParser::KeywordContext *ctx) = 0;

  virtual void enterName(NormalSQLParser::NameContext *ctx) = 0;
  virtual void exitName(NormalSQLParser::NameContext *ctx) = 0;

  virtual void enterFunction_name(NormalSQLParser::Function_nameContext *ctx) = 0;
  virtual void exitFunction_name(NormalSQLParser::Function_nameContext *ctx) = 0;

  virtual void enterDatabase_name(NormalSQLParser::Database_nameContext *ctx) = 0;
  virtual void exitDatabase_name(NormalSQLParser::Database_nameContext *ctx) = 0;

  virtual void enterTable_name(NormalSQLParser::Table_nameContext *ctx) = 0;
  virtual void exitTable_name(NormalSQLParser::Table_nameContext *ctx) = 0;

  virtual void enterTable_or_index_name(NormalSQLParser::Table_or_index_nameContext *ctx) = 0;
  virtual void exitTable_or_index_name(NormalSQLParser::Table_or_index_nameContext *ctx) = 0;

  virtual void enterNew_table_name(NormalSQLParser::New_table_nameContext *ctx) = 0;
  virtual void exitNew_table_name(NormalSQLParser::New_table_nameContext *ctx) = 0;

  virtual void enterColumn_name(NormalSQLParser::Column_nameContext *ctx) = 0;
  virtual void exitColumn_name(NormalSQLParser::Column_nameContext *ctx) = 0;

  virtual void enterCollation_name(NormalSQLParser::Collation_nameContext *ctx) = 0;
  virtual void exitCollation_name(NormalSQLParser::Collation_nameContext *ctx) = 0;

  virtual void enterForeign_table(NormalSQLParser::Foreign_tableContext *ctx) = 0;
  virtual void exitForeign_table(NormalSQLParser::Foreign_tableContext *ctx) = 0;

  virtual void enterIndex_name(NormalSQLParser::Index_nameContext *ctx) = 0;
  virtual void exitIndex_name(NormalSQLParser::Index_nameContext *ctx) = 0;

  virtual void enterTrigger_name(NormalSQLParser::Trigger_nameContext *ctx) = 0;
  virtual void exitTrigger_name(NormalSQLParser::Trigger_nameContext *ctx) = 0;

  virtual void enterView_name(NormalSQLParser::View_nameContext *ctx) = 0;
  virtual void exitView_name(NormalSQLParser::View_nameContext *ctx) = 0;

  virtual void enterModule_name(NormalSQLParser::Module_nameContext *ctx) = 0;
  virtual void exitModule_name(NormalSQLParser::Module_nameContext *ctx) = 0;

  virtual void enterPragma_name(NormalSQLParser::Pragma_nameContext *ctx) = 0;
  virtual void exitPragma_name(NormalSQLParser::Pragma_nameContext *ctx) = 0;

  virtual void enterSavepoint_name(NormalSQLParser::Savepoint_nameContext *ctx) = 0;
  virtual void exitSavepoint_name(NormalSQLParser::Savepoint_nameContext *ctx) = 0;

  virtual void enterTable_alias(NormalSQLParser::Table_aliasContext *ctx) = 0;
  virtual void exitTable_alias(NormalSQLParser::Table_aliasContext *ctx) = 0;

  virtual void enterTransaction_name(NormalSQLParser::Transaction_nameContext *ctx) = 0;
  virtual void exitTransaction_name(NormalSQLParser::Transaction_nameContext *ctx) = 0;

  virtual void enterAny_name(NormalSQLParser::Any_nameContext *ctx) = 0;
  virtual void exitAny_name(NormalSQLParser::Any_nameContext *ctx) = 0;


};

}  // namespace normal::sql
