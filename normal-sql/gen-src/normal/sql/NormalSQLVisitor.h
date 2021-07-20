
// Generated from /Users/yyf/Desktop/pushdownDB/normal/normal-sql/grammar/NormalSQL.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"
#include "NormalSQLParser.h"


namespace normal::sql {

/**
 * This class defines an abstract visitor for a parse tree
 * produced by NormalSQLParser.
 */
class  NormalSQLVisitor : public antlr4::tree::AbstractParseTreeVisitor {
public:

  /**
   * Visit parse trees produced by NormalSQLParser.
   */
    virtual antlrcpp::Any visitParse(NormalSQLParser::ParseContext *context) = 0;

    virtual antlrcpp::Any visitError(NormalSQLParser::ErrorContext *context) = 0;

    virtual antlrcpp::Any visitSql_stmt_list(NormalSQLParser::Sql_stmt_listContext *context) = 0;

    virtual antlrcpp::Any visitSql_stmt(NormalSQLParser::Sql_stmtContext *context) = 0;

    virtual antlrcpp::Any visitSelect_stmt(NormalSQLParser::Select_stmtContext *context) = 0;

    virtual antlrcpp::Any visitSelect_core(NormalSQLParser::Select_coreContext *context) = 0;

    virtual antlrcpp::Any visitFrom_clause(NormalSQLParser::From_clauseContext *context) = 0;

    virtual antlrcpp::Any visitWhere_clause(NormalSQLParser::Where_clauseContext *context) = 0;

    virtual antlrcpp::Any visitGroupBy_clause(NormalSQLParser::GroupBy_clauseContext *context) = 0;

    virtual antlrcpp::Any visitType_name(NormalSQLParser::Type_nameContext *context) = 0;

    virtual antlrcpp::Any visitExpr_comp(NormalSQLParser::Expr_compContext *context) = 0;

    virtual antlrcpp::Any visitExpr_case(NormalSQLParser::Expr_caseContext *context) = 0;

    virtual antlrcpp::Any visitExpr_raise_function(NormalSQLParser::Expr_raise_functionContext *context) = 0;

    virtual antlrcpp::Any visitExpr_parens(NormalSQLParser::Expr_parensContext *context) = 0;

    virtual antlrcpp::Any visitExpr_column(NormalSQLParser::Expr_columnContext *context) = 0;

    virtual antlrcpp::Any visitExpr_function(NormalSQLParser::Expr_functionContext *context) = 0;

    virtual antlrcpp::Any visitExpr_eq(NormalSQLParser::Expr_eqContext *context) = 0;

    virtual antlrcpp::Any visitExpr_null(NormalSQLParser::Expr_nullContext *context) = 0;

    virtual antlrcpp::Any visitExpr_bind_parameter(NormalSQLParser::Expr_bind_parameterContext *context) = 0;

    virtual antlrcpp::Any visitExpr_is(NormalSQLParser::Expr_isContext *context) = 0;

    virtual antlrcpp::Any visitExpr_literal(NormalSQLParser::Expr_literalContext *context) = 0;

    virtual antlrcpp::Any visitExpr_cast(NormalSQLParser::Expr_castContext *context) = 0;

    virtual antlrcpp::Any visitExpr_exists(NormalSQLParser::Expr_existsContext *context) = 0;

    virtual antlrcpp::Any visitExpr_mul_div_mod(NormalSQLParser::Expr_mul_div_modContext *context) = 0;

    virtual antlrcpp::Any visitExpr_between(NormalSQLParser::Expr_betweenContext *context) = 0;

    virtual antlrcpp::Any visitExpr_in(NormalSQLParser::Expr_inContext *context) = 0;

    virtual antlrcpp::Any visitExpr_bit(NormalSQLParser::Expr_bitContext *context) = 0;

    virtual antlrcpp::Any visitExpr_or(NormalSQLParser::Expr_orContext *context) = 0;

    virtual antlrcpp::Any visitExpr_collate(NormalSQLParser::Expr_collateContext *context) = 0;

    virtual antlrcpp::Any visitExpr_and(NormalSQLParser::Expr_andContext *context) = 0;

    virtual antlrcpp::Any visitExpr_add_sub(NormalSQLParser::Expr_add_subContext *context) = 0;

    virtual antlrcpp::Any visitExpr_unary(NormalSQLParser::Expr_unaryContext *context) = 0;

    virtual antlrcpp::Any visitExpr_str_match(NormalSQLParser::Expr_str_matchContext *context) = 0;

    virtual antlrcpp::Any visitRaise_function(NormalSQLParser::Raise_functionContext *context) = 0;

    virtual antlrcpp::Any visitIndexed_column(NormalSQLParser::Indexed_columnContext *context) = 0;

    virtual antlrcpp::Any visitWith_clause(NormalSQLParser::With_clauseContext *context) = 0;

    virtual antlrcpp::Any visitQualified_table_name(NormalSQLParser::Qualified_table_nameContext *context) = 0;

    virtual antlrcpp::Any visitOrdering_term(NormalSQLParser::Ordering_termContext *context) = 0;

    virtual antlrcpp::Any visitPragma_value(NormalSQLParser::Pragma_valueContext *context) = 0;

    virtual antlrcpp::Any visitCommon_table_expression(NormalSQLParser::Common_table_expressionContext *context) = 0;

    virtual antlrcpp::Any visitResult_column(NormalSQLParser::Result_columnContext *context) = 0;

    virtual antlrcpp::Any visitTable_or_subquery(NormalSQLParser::Table_or_subqueryContext *context) = 0;

    virtual antlrcpp::Any visitJoin_clause(NormalSQLParser::Join_clauseContext *context) = 0;

    virtual antlrcpp::Any visitJoin_operator(NormalSQLParser::Join_operatorContext *context) = 0;

    virtual antlrcpp::Any visitJoin_constraint(NormalSQLParser::Join_constraintContext *context) = 0;

    virtual antlrcpp::Any visitCompound_operator(NormalSQLParser::Compound_operatorContext *context) = 0;

    virtual antlrcpp::Any visitCte_table_name(NormalSQLParser::Cte_table_nameContext *context) = 0;

    virtual antlrcpp::Any visitSigned_number(NormalSQLParser::Signed_numberContext *context) = 0;

    virtual antlrcpp::Any visitLiteral_value_numeric(NormalSQLParser::Literal_value_numericContext *context) = 0;

    virtual antlrcpp::Any visitLiteral_value_string(NormalSQLParser::Literal_value_stringContext *context) = 0;

    virtual antlrcpp::Any visitLiteral_value_blob(NormalSQLParser::Literal_value_blobContext *context) = 0;

    virtual antlrcpp::Any visitLiteral_value_k_null(NormalSQLParser::Literal_value_k_nullContext *context) = 0;

    virtual antlrcpp::Any visitLiteral_value_k_current_time(NormalSQLParser::Literal_value_k_current_timeContext *context) = 0;

    virtual antlrcpp::Any visitLiteral_value_k_current_date(NormalSQLParser::Literal_value_k_current_dateContext *context) = 0;

    virtual antlrcpp::Any visitLiteral_value_k_current_timestamp(NormalSQLParser::Literal_value_k_current_timestampContext *context) = 0;

    virtual antlrcpp::Any visitUnary_operator(NormalSQLParser::Unary_operatorContext *context) = 0;

    virtual antlrcpp::Any visitError_message(NormalSQLParser::Error_messageContext *context) = 0;

    virtual antlrcpp::Any visitColumn_alias(NormalSQLParser::Column_aliasContext *context) = 0;

    virtual antlrcpp::Any visitKeyword(NormalSQLParser::KeywordContext *context) = 0;

    virtual antlrcpp::Any visitName(NormalSQLParser::NameContext *context) = 0;

    virtual antlrcpp::Any visitFunction_name(NormalSQLParser::Function_nameContext *context) = 0;

    virtual antlrcpp::Any visitDatabase_name(NormalSQLParser::Database_nameContext *context) = 0;

    virtual antlrcpp::Any visitTable_name(NormalSQLParser::Table_nameContext *context) = 0;

    virtual antlrcpp::Any visitTable_or_index_name(NormalSQLParser::Table_or_index_nameContext *context) = 0;

    virtual antlrcpp::Any visitNew_table_name(NormalSQLParser::New_table_nameContext *context) = 0;

    virtual antlrcpp::Any visitColumn_name(NormalSQLParser::Column_nameContext *context) = 0;

    virtual antlrcpp::Any visitCollation_name(NormalSQLParser::Collation_nameContext *context) = 0;

    virtual antlrcpp::Any visitForeign_table(NormalSQLParser::Foreign_tableContext *context) = 0;

    virtual antlrcpp::Any visitIndex_name(NormalSQLParser::Index_nameContext *context) = 0;

    virtual antlrcpp::Any visitTrigger_name(NormalSQLParser::Trigger_nameContext *context) = 0;

    virtual antlrcpp::Any visitView_name(NormalSQLParser::View_nameContext *context) = 0;

    virtual antlrcpp::Any visitModule_name(NormalSQLParser::Module_nameContext *context) = 0;

    virtual antlrcpp::Any visitPragma_name(NormalSQLParser::Pragma_nameContext *context) = 0;

    virtual antlrcpp::Any visitSavepoint_name(NormalSQLParser::Savepoint_nameContext *context) = 0;

    virtual antlrcpp::Any visitTable_alias(NormalSQLParser::Table_aliasContext *context) = 0;

    virtual antlrcpp::Any visitTransaction_name(NormalSQLParser::Transaction_nameContext *context) = 0;

    virtual antlrcpp::Any visitAny_name(NormalSQLParser::Any_nameContext *context) = 0;


};

}  // namespace normal::sql
