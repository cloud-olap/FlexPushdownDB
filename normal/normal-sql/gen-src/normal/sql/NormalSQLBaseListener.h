
// Generated from /Users/yyf/Desktop/pushdownDB/normal/normal-sql/grammar/NormalSQL.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"
#include "NormalSQLListener.h"


namespace normal::sql {

/**
 * This class provides an empty implementation of NormalSQLListener,
 * which can be extended to create a listener which only needs to handle a subset
 * of the available methods.
 */
class  NormalSQLBaseListener : public NormalSQLListener {
public:

  virtual void enterParse(NormalSQLParser::ParseContext * /*ctx*/) override { }
  virtual void exitParse(NormalSQLParser::ParseContext * /*ctx*/) override { }

  virtual void enterError(NormalSQLParser::ErrorContext * /*ctx*/) override { }
  virtual void exitError(NormalSQLParser::ErrorContext * /*ctx*/) override { }

  virtual void enterSql_stmt_list(NormalSQLParser::Sql_stmt_listContext * /*ctx*/) override { }
  virtual void exitSql_stmt_list(NormalSQLParser::Sql_stmt_listContext * /*ctx*/) override { }

  virtual void enterSql_stmt(NormalSQLParser::Sql_stmtContext * /*ctx*/) override { }
  virtual void exitSql_stmt(NormalSQLParser::Sql_stmtContext * /*ctx*/) override { }

  virtual void enterSelect_stmt(NormalSQLParser::Select_stmtContext * /*ctx*/) override { }
  virtual void exitSelect_stmt(NormalSQLParser::Select_stmtContext * /*ctx*/) override { }

  virtual void enterSelect_core(NormalSQLParser::Select_coreContext * /*ctx*/) override { }
  virtual void exitSelect_core(NormalSQLParser::Select_coreContext * /*ctx*/) override { }

  virtual void enterFrom_clause(NormalSQLParser::From_clauseContext * /*ctx*/) override { }
  virtual void exitFrom_clause(NormalSQLParser::From_clauseContext * /*ctx*/) override { }

  virtual void enterWhere_clause(NormalSQLParser::Where_clauseContext * /*ctx*/) override { }
  virtual void exitWhere_clause(NormalSQLParser::Where_clauseContext * /*ctx*/) override { }

  virtual void enterGroupBy_clause(NormalSQLParser::GroupBy_clauseContext * /*ctx*/) override { }
  virtual void exitGroupBy_clause(NormalSQLParser::GroupBy_clauseContext * /*ctx*/) override { }

  virtual void enterType_name(NormalSQLParser::Type_nameContext * /*ctx*/) override { }
  virtual void exitType_name(NormalSQLParser::Type_nameContext * /*ctx*/) override { }

  virtual void enterExpr_comp(NormalSQLParser::Expr_compContext * /*ctx*/) override { }
  virtual void exitExpr_comp(NormalSQLParser::Expr_compContext * /*ctx*/) override { }

  virtual void enterExpr_case(NormalSQLParser::Expr_caseContext * /*ctx*/) override { }
  virtual void exitExpr_case(NormalSQLParser::Expr_caseContext * /*ctx*/) override { }

  virtual void enterExpr_raise_function(NormalSQLParser::Expr_raise_functionContext * /*ctx*/) override { }
  virtual void exitExpr_raise_function(NormalSQLParser::Expr_raise_functionContext * /*ctx*/) override { }

  virtual void enterExpr_parens(NormalSQLParser::Expr_parensContext * /*ctx*/) override { }
  virtual void exitExpr_parens(NormalSQLParser::Expr_parensContext * /*ctx*/) override { }

  virtual void enterExpr_column(NormalSQLParser::Expr_columnContext * /*ctx*/) override { }
  virtual void exitExpr_column(NormalSQLParser::Expr_columnContext * /*ctx*/) override { }

  virtual void enterExpr_function(NormalSQLParser::Expr_functionContext * /*ctx*/) override { }
  virtual void exitExpr_function(NormalSQLParser::Expr_functionContext * /*ctx*/) override { }

  virtual void enterExpr_eq(NormalSQLParser::Expr_eqContext * /*ctx*/) override { }
  virtual void exitExpr_eq(NormalSQLParser::Expr_eqContext * /*ctx*/) override { }

  virtual void enterExpr_null(NormalSQLParser::Expr_nullContext * /*ctx*/) override { }
  virtual void exitExpr_null(NormalSQLParser::Expr_nullContext * /*ctx*/) override { }

  virtual void enterExpr_bind_parameter(NormalSQLParser::Expr_bind_parameterContext * /*ctx*/) override { }
  virtual void exitExpr_bind_parameter(NormalSQLParser::Expr_bind_parameterContext * /*ctx*/) override { }

  virtual void enterExpr_is(NormalSQLParser::Expr_isContext * /*ctx*/) override { }
  virtual void exitExpr_is(NormalSQLParser::Expr_isContext * /*ctx*/) override { }

  virtual void enterExpr_literal(NormalSQLParser::Expr_literalContext * /*ctx*/) override { }
  virtual void exitExpr_literal(NormalSQLParser::Expr_literalContext * /*ctx*/) override { }

  virtual void enterExpr_cast(NormalSQLParser::Expr_castContext * /*ctx*/) override { }
  virtual void exitExpr_cast(NormalSQLParser::Expr_castContext * /*ctx*/) override { }

  virtual void enterExpr_exists(NormalSQLParser::Expr_existsContext * /*ctx*/) override { }
  virtual void exitExpr_exists(NormalSQLParser::Expr_existsContext * /*ctx*/) override { }

  virtual void enterExpr_mul_div_mod(NormalSQLParser::Expr_mul_div_modContext * /*ctx*/) override { }
  virtual void exitExpr_mul_div_mod(NormalSQLParser::Expr_mul_div_modContext * /*ctx*/) override { }

  virtual void enterExpr_between(NormalSQLParser::Expr_betweenContext * /*ctx*/) override { }
  virtual void exitExpr_between(NormalSQLParser::Expr_betweenContext * /*ctx*/) override { }

  virtual void enterExpr_in(NormalSQLParser::Expr_inContext * /*ctx*/) override { }
  virtual void exitExpr_in(NormalSQLParser::Expr_inContext * /*ctx*/) override { }

  virtual void enterExpr_bit(NormalSQLParser::Expr_bitContext * /*ctx*/) override { }
  virtual void exitExpr_bit(NormalSQLParser::Expr_bitContext * /*ctx*/) override { }

  virtual void enterExpr_or(NormalSQLParser::Expr_orContext * /*ctx*/) override { }
  virtual void exitExpr_or(NormalSQLParser::Expr_orContext * /*ctx*/) override { }

  virtual void enterExpr_collate(NormalSQLParser::Expr_collateContext * /*ctx*/) override { }
  virtual void exitExpr_collate(NormalSQLParser::Expr_collateContext * /*ctx*/) override { }

  virtual void enterExpr_and(NormalSQLParser::Expr_andContext * /*ctx*/) override { }
  virtual void exitExpr_and(NormalSQLParser::Expr_andContext * /*ctx*/) override { }

  virtual void enterExpr_add_sub(NormalSQLParser::Expr_add_subContext * /*ctx*/) override { }
  virtual void exitExpr_add_sub(NormalSQLParser::Expr_add_subContext * /*ctx*/) override { }

  virtual void enterExpr_unary(NormalSQLParser::Expr_unaryContext * /*ctx*/) override { }
  virtual void exitExpr_unary(NormalSQLParser::Expr_unaryContext * /*ctx*/) override { }

  virtual void enterExpr_str_match(NormalSQLParser::Expr_str_matchContext * /*ctx*/) override { }
  virtual void exitExpr_str_match(NormalSQLParser::Expr_str_matchContext * /*ctx*/) override { }

  virtual void enterRaise_function(NormalSQLParser::Raise_functionContext * /*ctx*/) override { }
  virtual void exitRaise_function(NormalSQLParser::Raise_functionContext * /*ctx*/) override { }

  virtual void enterIndexed_column(NormalSQLParser::Indexed_columnContext * /*ctx*/) override { }
  virtual void exitIndexed_column(NormalSQLParser::Indexed_columnContext * /*ctx*/) override { }

  virtual void enterWith_clause(NormalSQLParser::With_clauseContext * /*ctx*/) override { }
  virtual void exitWith_clause(NormalSQLParser::With_clauseContext * /*ctx*/) override { }

  virtual void enterQualified_table_name(NormalSQLParser::Qualified_table_nameContext * /*ctx*/) override { }
  virtual void exitQualified_table_name(NormalSQLParser::Qualified_table_nameContext * /*ctx*/) override { }

  virtual void enterOrdering_term(NormalSQLParser::Ordering_termContext * /*ctx*/) override { }
  virtual void exitOrdering_term(NormalSQLParser::Ordering_termContext * /*ctx*/) override { }

  virtual void enterPragma_value(NormalSQLParser::Pragma_valueContext * /*ctx*/) override { }
  virtual void exitPragma_value(NormalSQLParser::Pragma_valueContext * /*ctx*/) override { }

  virtual void enterCommon_table_expression(NormalSQLParser::Common_table_expressionContext * /*ctx*/) override { }
  virtual void exitCommon_table_expression(NormalSQLParser::Common_table_expressionContext * /*ctx*/) override { }

  virtual void enterResult_column(NormalSQLParser::Result_columnContext * /*ctx*/) override { }
  virtual void exitResult_column(NormalSQLParser::Result_columnContext * /*ctx*/) override { }

  virtual void enterTable_or_subquery(NormalSQLParser::Table_or_subqueryContext * /*ctx*/) override { }
  virtual void exitTable_or_subquery(NormalSQLParser::Table_or_subqueryContext * /*ctx*/) override { }

  virtual void enterJoin_clause(NormalSQLParser::Join_clauseContext * /*ctx*/) override { }
  virtual void exitJoin_clause(NormalSQLParser::Join_clauseContext * /*ctx*/) override { }

  virtual void enterJoin_operator(NormalSQLParser::Join_operatorContext * /*ctx*/) override { }
  virtual void exitJoin_operator(NormalSQLParser::Join_operatorContext * /*ctx*/) override { }

  virtual void enterJoin_constraint(NormalSQLParser::Join_constraintContext * /*ctx*/) override { }
  virtual void exitJoin_constraint(NormalSQLParser::Join_constraintContext * /*ctx*/) override { }

  virtual void enterCompound_operator(NormalSQLParser::Compound_operatorContext * /*ctx*/) override { }
  virtual void exitCompound_operator(NormalSQLParser::Compound_operatorContext * /*ctx*/) override { }

  virtual void enterCte_table_name(NormalSQLParser::Cte_table_nameContext * /*ctx*/) override { }
  virtual void exitCte_table_name(NormalSQLParser::Cte_table_nameContext * /*ctx*/) override { }

  virtual void enterSigned_number(NormalSQLParser::Signed_numberContext * /*ctx*/) override { }
  virtual void exitSigned_number(NormalSQLParser::Signed_numberContext * /*ctx*/) override { }

  virtual void enterLiteral_value_numeric(NormalSQLParser::Literal_value_numericContext * /*ctx*/) override { }
  virtual void exitLiteral_value_numeric(NormalSQLParser::Literal_value_numericContext * /*ctx*/) override { }

  virtual void enterLiteral_value_string(NormalSQLParser::Literal_value_stringContext * /*ctx*/) override { }
  virtual void exitLiteral_value_string(NormalSQLParser::Literal_value_stringContext * /*ctx*/) override { }

  virtual void enterLiteral_value_blob(NormalSQLParser::Literal_value_blobContext * /*ctx*/) override { }
  virtual void exitLiteral_value_blob(NormalSQLParser::Literal_value_blobContext * /*ctx*/) override { }

  virtual void enterLiteral_value_k_null(NormalSQLParser::Literal_value_k_nullContext * /*ctx*/) override { }
  virtual void exitLiteral_value_k_null(NormalSQLParser::Literal_value_k_nullContext * /*ctx*/) override { }

  virtual void enterLiteral_value_k_current_time(NormalSQLParser::Literal_value_k_current_timeContext * /*ctx*/) override { }
  virtual void exitLiteral_value_k_current_time(NormalSQLParser::Literal_value_k_current_timeContext * /*ctx*/) override { }

  virtual void enterLiteral_value_k_current_date(NormalSQLParser::Literal_value_k_current_dateContext * /*ctx*/) override { }
  virtual void exitLiteral_value_k_current_date(NormalSQLParser::Literal_value_k_current_dateContext * /*ctx*/) override { }

  virtual void enterLiteral_value_k_current_timestamp(NormalSQLParser::Literal_value_k_current_timestampContext * /*ctx*/) override { }
  virtual void exitLiteral_value_k_current_timestamp(NormalSQLParser::Literal_value_k_current_timestampContext * /*ctx*/) override { }

  virtual void enterUnary_operator(NormalSQLParser::Unary_operatorContext * /*ctx*/) override { }
  virtual void exitUnary_operator(NormalSQLParser::Unary_operatorContext * /*ctx*/) override { }

  virtual void enterError_message(NormalSQLParser::Error_messageContext * /*ctx*/) override { }
  virtual void exitError_message(NormalSQLParser::Error_messageContext * /*ctx*/) override { }

  virtual void enterColumn_alias(NormalSQLParser::Column_aliasContext * /*ctx*/) override { }
  virtual void exitColumn_alias(NormalSQLParser::Column_aliasContext * /*ctx*/) override { }

  virtual void enterKeyword(NormalSQLParser::KeywordContext * /*ctx*/) override { }
  virtual void exitKeyword(NormalSQLParser::KeywordContext * /*ctx*/) override { }

  virtual void enterName(NormalSQLParser::NameContext * /*ctx*/) override { }
  virtual void exitName(NormalSQLParser::NameContext * /*ctx*/) override { }

  virtual void enterFunction_name(NormalSQLParser::Function_nameContext * /*ctx*/) override { }
  virtual void exitFunction_name(NormalSQLParser::Function_nameContext * /*ctx*/) override { }

  virtual void enterDatabase_name(NormalSQLParser::Database_nameContext * /*ctx*/) override { }
  virtual void exitDatabase_name(NormalSQLParser::Database_nameContext * /*ctx*/) override { }

  virtual void enterTable_name(NormalSQLParser::Table_nameContext * /*ctx*/) override { }
  virtual void exitTable_name(NormalSQLParser::Table_nameContext * /*ctx*/) override { }

  virtual void enterTable_or_index_name(NormalSQLParser::Table_or_index_nameContext * /*ctx*/) override { }
  virtual void exitTable_or_index_name(NormalSQLParser::Table_or_index_nameContext * /*ctx*/) override { }

  virtual void enterNew_table_name(NormalSQLParser::New_table_nameContext * /*ctx*/) override { }
  virtual void exitNew_table_name(NormalSQLParser::New_table_nameContext * /*ctx*/) override { }

  virtual void enterColumn_name(NormalSQLParser::Column_nameContext * /*ctx*/) override { }
  virtual void exitColumn_name(NormalSQLParser::Column_nameContext * /*ctx*/) override { }

  virtual void enterCollation_name(NormalSQLParser::Collation_nameContext * /*ctx*/) override { }
  virtual void exitCollation_name(NormalSQLParser::Collation_nameContext * /*ctx*/) override { }

  virtual void enterForeign_table(NormalSQLParser::Foreign_tableContext * /*ctx*/) override { }
  virtual void exitForeign_table(NormalSQLParser::Foreign_tableContext * /*ctx*/) override { }

  virtual void enterIndex_name(NormalSQLParser::Index_nameContext * /*ctx*/) override { }
  virtual void exitIndex_name(NormalSQLParser::Index_nameContext * /*ctx*/) override { }

  virtual void enterTrigger_name(NormalSQLParser::Trigger_nameContext * /*ctx*/) override { }
  virtual void exitTrigger_name(NormalSQLParser::Trigger_nameContext * /*ctx*/) override { }

  virtual void enterView_name(NormalSQLParser::View_nameContext * /*ctx*/) override { }
  virtual void exitView_name(NormalSQLParser::View_nameContext * /*ctx*/) override { }

  virtual void enterModule_name(NormalSQLParser::Module_nameContext * /*ctx*/) override { }
  virtual void exitModule_name(NormalSQLParser::Module_nameContext * /*ctx*/) override { }

  virtual void enterPragma_name(NormalSQLParser::Pragma_nameContext * /*ctx*/) override { }
  virtual void exitPragma_name(NormalSQLParser::Pragma_nameContext * /*ctx*/) override { }

  virtual void enterSavepoint_name(NormalSQLParser::Savepoint_nameContext * /*ctx*/) override { }
  virtual void exitSavepoint_name(NormalSQLParser::Savepoint_nameContext * /*ctx*/) override { }

  virtual void enterTable_alias(NormalSQLParser::Table_aliasContext * /*ctx*/) override { }
  virtual void exitTable_alias(NormalSQLParser::Table_aliasContext * /*ctx*/) override { }

  virtual void enterTransaction_name(NormalSQLParser::Transaction_nameContext * /*ctx*/) override { }
  virtual void exitTransaction_name(NormalSQLParser::Transaction_nameContext * /*ctx*/) override { }

  virtual void enterAny_name(NormalSQLParser::Any_nameContext * /*ctx*/) override { }
  virtual void exitAny_name(NormalSQLParser::Any_nameContext * /*ctx*/) override { }


  virtual void enterEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void exitEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void visitTerminal(antlr4::tree::TerminalNode * /*node*/) override { }
  virtual void visitErrorNode(antlr4::tree::ErrorNode * /*node*/) override { }

};

}  // namespace normal::sql
