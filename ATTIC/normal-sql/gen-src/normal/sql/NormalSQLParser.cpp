
// Generated from /Users/yyf/Desktop/pushdownDB/normal/normal-sql/grammar/NormalSQL.g4 by ANTLR 4.8


#include "NormalSQLListener.h"
#include "NormalSQLVisitor.h"

#include "NormalSQLParser.h"


using namespace antlrcpp;
using namespace normal::sql;
using namespace antlr4;

NormalSQLParser::NormalSQLParser(TokenStream *input) : Parser(input) {
  _interpreter = new atn::ParserATNSimulator(this, _atn, _decisionToDFA, _sharedContextCache);
}

NormalSQLParser::~NormalSQLParser() {
  delete _interpreter;
}

std::string NormalSQLParser::getGrammarFileName() const {
  return "NormalSQL.g4";
}

const std::vector<std::string>& NormalSQLParser::getRuleNames() const {
  return _ruleNames;
}

dfa::Vocabulary& NormalSQLParser::getVocabulary() const {
  return _vocabulary;
}


//----------------- ParseContext ------------------------------------------------------------------

NormalSQLParser::ParseContext::ParseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::ParseContext::EOF() {
  return getToken(NormalSQLParser::EOF, 0);
}

std::vector<NormalSQLParser::Sql_stmt_listContext *> NormalSQLParser::ParseContext::sql_stmt_list() {
  return getRuleContexts<NormalSQLParser::Sql_stmt_listContext>();
}

NormalSQLParser::Sql_stmt_listContext* NormalSQLParser::ParseContext::sql_stmt_list(size_t i) {
  return getRuleContext<NormalSQLParser::Sql_stmt_listContext>(i);
}

std::vector<NormalSQLParser::ErrorContext *> NormalSQLParser::ParseContext::error() {
  return getRuleContexts<NormalSQLParser::ErrorContext>();
}

NormalSQLParser::ErrorContext* NormalSQLParser::ParseContext::error(size_t i) {
  return getRuleContext<NormalSQLParser::ErrorContext>(i);
}


size_t NormalSQLParser::ParseContext::getRuleIndex() const {
  return NormalSQLParser::RuleParse;
}

void NormalSQLParser::ParseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterParse(this);
}

void NormalSQLParser::ParseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitParse(this);
}


antlrcpp::Any NormalSQLParser::ParseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitParse(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::ParseContext* NormalSQLParser::parse() {
  ParseContext *_localctx = _tracker.createInstance<ParseContext>(_ctx, getState());
  enterRule(_localctx, 0, NormalSQLParser::RuleParse);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(102);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == NormalSQLParser::SCOL || _la == NormalSQLParser::K_EXPLAIN

    || _la == NormalSQLParser::K_SELECT || ((((_la - 143) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 143)) & ((1ULL << (NormalSQLParser::K_VALUES - 143))
      | (1ULL << (NormalSQLParser::K_WITH - 143))
      | (1ULL << (NormalSQLParser::UNEXPECTED_CHAR - 143)))) != 0)) {
      setState(100);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case NormalSQLParser::SCOL:
        case NormalSQLParser::K_EXPLAIN:
        case NormalSQLParser::K_SELECT:
        case NormalSQLParser::K_VALUES:
        case NormalSQLParser::K_WITH: {
          setState(98);
          sql_stmt_list();
          break;
        }

        case NormalSQLParser::UNEXPECTED_CHAR: {
          setState(99);
          error();
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(104);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(105);
    match(NormalSQLParser::EOF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ErrorContext ------------------------------------------------------------------

NormalSQLParser::ErrorContext::ErrorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::ErrorContext::UNEXPECTED_CHAR() {
  return getToken(NormalSQLParser::UNEXPECTED_CHAR, 0);
}


size_t NormalSQLParser::ErrorContext::getRuleIndex() const {
  return NormalSQLParser::RuleError;
}

void NormalSQLParser::ErrorContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterError(this);
}

void NormalSQLParser::ErrorContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitError(this);
}


antlrcpp::Any NormalSQLParser::ErrorContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitError(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::ErrorContext* NormalSQLParser::error() {
  ErrorContext *_localctx = _tracker.createInstance<ErrorContext>(_ctx, getState());
  enterRule(_localctx, 2, NormalSQLParser::RuleError);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(107);
    dynamic_cast<ErrorContext *>(_localctx)->unexpected_charToken = match(NormalSQLParser::UNEXPECTED_CHAR);

         throw new RuntimeException("UNEXPECTED_CHAR=" + (dynamic_cast<ErrorContext *>(_localctx)->unexpected_charToken != nullptr ? dynamic_cast<ErrorContext *>(_localctx)->unexpected_charToken->getText() : ""));
       
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Sql_stmt_listContext ------------------------------------------------------------------

NormalSQLParser::Sql_stmt_listContext::Sql_stmt_listContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<NormalSQLParser::Sql_stmtContext *> NormalSQLParser::Sql_stmt_listContext::sql_stmt() {
  return getRuleContexts<NormalSQLParser::Sql_stmtContext>();
}

NormalSQLParser::Sql_stmtContext* NormalSQLParser::Sql_stmt_listContext::sql_stmt(size_t i) {
  return getRuleContext<NormalSQLParser::Sql_stmtContext>(i);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Sql_stmt_listContext::SCOL() {
  return getTokens(NormalSQLParser::SCOL);
}

tree::TerminalNode* NormalSQLParser::Sql_stmt_listContext::SCOL(size_t i) {
  return getToken(NormalSQLParser::SCOL, i);
}


size_t NormalSQLParser::Sql_stmt_listContext::getRuleIndex() const {
  return NormalSQLParser::RuleSql_stmt_list;
}

void NormalSQLParser::Sql_stmt_listContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSql_stmt_list(this);
}

void NormalSQLParser::Sql_stmt_listContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSql_stmt_list(this);
}


antlrcpp::Any NormalSQLParser::Sql_stmt_listContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitSql_stmt_list(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Sql_stmt_listContext* NormalSQLParser::sql_stmt_list() {
  Sql_stmt_listContext *_localctx = _tracker.createInstance<Sql_stmt_listContext>(_ctx, getState());
  enterRule(_localctx, 4, NormalSQLParser::RuleSql_stmt_list);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(113);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == NormalSQLParser::SCOL) {
      setState(110);
      match(NormalSQLParser::SCOL);
      setState(115);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(116);
    sql_stmt();
    setState(125);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 4, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(118); 
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
          setState(117);
          match(NormalSQLParser::SCOL);
          setState(120); 
          _errHandler->sync(this);
          _la = _input->LA(1);
        } while (_la == NormalSQLParser::SCOL);
        setState(122);
        sql_stmt(); 
      }
      setState(127);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 4, _ctx);
    }
    setState(131);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 5, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(128);
        match(NormalSQLParser::SCOL); 
      }
      setState(133);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 5, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Sql_stmtContext ------------------------------------------------------------------

NormalSQLParser::Sql_stmtContext::Sql_stmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Select_stmtContext* NormalSQLParser::Sql_stmtContext::select_stmt() {
  return getRuleContext<NormalSQLParser::Select_stmtContext>(0);
}

tree::TerminalNode* NormalSQLParser::Sql_stmtContext::K_EXPLAIN() {
  return getToken(NormalSQLParser::K_EXPLAIN, 0);
}

tree::TerminalNode* NormalSQLParser::Sql_stmtContext::K_QUERY() {
  return getToken(NormalSQLParser::K_QUERY, 0);
}

tree::TerminalNode* NormalSQLParser::Sql_stmtContext::K_PLAN() {
  return getToken(NormalSQLParser::K_PLAN, 0);
}


size_t NormalSQLParser::Sql_stmtContext::getRuleIndex() const {
  return NormalSQLParser::RuleSql_stmt;
}

void NormalSQLParser::Sql_stmtContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSql_stmt(this);
}

void NormalSQLParser::Sql_stmtContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSql_stmt(this);
}


antlrcpp::Any NormalSQLParser::Sql_stmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitSql_stmt(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Sql_stmtContext* NormalSQLParser::sql_stmt() {
  Sql_stmtContext *_localctx = _tracker.createInstance<Sql_stmtContext>(_ctx, getState());
  enterRule(_localctx, 6, NormalSQLParser::RuleSql_stmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(139);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NormalSQLParser::K_EXPLAIN) {
      setState(134);
      match(NormalSQLParser::K_EXPLAIN);
      setState(137);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NormalSQLParser::K_QUERY) {
        setState(135);
        match(NormalSQLParser::K_QUERY);
        setState(136);
        match(NormalSQLParser::K_PLAN);
      }
    }
    setState(141);
    select_stmt();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Select_stmtContext ------------------------------------------------------------------

NormalSQLParser::Select_stmtContext::Select_stmtContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<NormalSQLParser::Select_coreContext *> NormalSQLParser::Select_stmtContext::select_core() {
  return getRuleContexts<NormalSQLParser::Select_coreContext>();
}

NormalSQLParser::Select_coreContext* NormalSQLParser::Select_stmtContext::select_core(size_t i) {
  return getRuleContext<NormalSQLParser::Select_coreContext>(i);
}

tree::TerminalNode* NormalSQLParser::Select_stmtContext::K_WITH() {
  return getToken(NormalSQLParser::K_WITH, 0);
}

std::vector<NormalSQLParser::Common_table_expressionContext *> NormalSQLParser::Select_stmtContext::common_table_expression() {
  return getRuleContexts<NormalSQLParser::Common_table_expressionContext>();
}

NormalSQLParser::Common_table_expressionContext* NormalSQLParser::Select_stmtContext::common_table_expression(size_t i) {
  return getRuleContext<NormalSQLParser::Common_table_expressionContext>(i);
}

std::vector<NormalSQLParser::Compound_operatorContext *> NormalSQLParser::Select_stmtContext::compound_operator() {
  return getRuleContexts<NormalSQLParser::Compound_operatorContext>();
}

NormalSQLParser::Compound_operatorContext* NormalSQLParser::Select_stmtContext::compound_operator(size_t i) {
  return getRuleContext<NormalSQLParser::Compound_operatorContext>(i);
}

tree::TerminalNode* NormalSQLParser::Select_stmtContext::K_ORDER() {
  return getToken(NormalSQLParser::K_ORDER, 0);
}

tree::TerminalNode* NormalSQLParser::Select_stmtContext::K_BY() {
  return getToken(NormalSQLParser::K_BY, 0);
}

std::vector<NormalSQLParser::Ordering_termContext *> NormalSQLParser::Select_stmtContext::ordering_term() {
  return getRuleContexts<NormalSQLParser::Ordering_termContext>();
}

NormalSQLParser::Ordering_termContext* NormalSQLParser::Select_stmtContext::ordering_term(size_t i) {
  return getRuleContext<NormalSQLParser::Ordering_termContext>(i);
}

tree::TerminalNode* NormalSQLParser::Select_stmtContext::K_LIMIT() {
  return getToken(NormalSQLParser::K_LIMIT, 0);
}

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::Select_stmtContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::Select_stmtContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

tree::TerminalNode* NormalSQLParser::Select_stmtContext::K_RECURSIVE() {
  return getToken(NormalSQLParser::K_RECURSIVE, 0);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Select_stmtContext::COMMA() {
  return getTokens(NormalSQLParser::COMMA);
}

tree::TerminalNode* NormalSQLParser::Select_stmtContext::COMMA(size_t i) {
  return getToken(NormalSQLParser::COMMA, i);
}

tree::TerminalNode* NormalSQLParser::Select_stmtContext::K_OFFSET() {
  return getToken(NormalSQLParser::K_OFFSET, 0);
}


size_t NormalSQLParser::Select_stmtContext::getRuleIndex() const {
  return NormalSQLParser::RuleSelect_stmt;
}

void NormalSQLParser::Select_stmtContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSelect_stmt(this);
}

void NormalSQLParser::Select_stmtContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSelect_stmt(this);
}


antlrcpp::Any NormalSQLParser::Select_stmtContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitSelect_stmt(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Select_stmtContext* NormalSQLParser::select_stmt() {
  Select_stmtContext *_localctx = _tracker.createInstance<Select_stmtContext>(_ctx, getState());
  enterRule(_localctx, 8, NormalSQLParser::RuleSelect_stmt);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(155);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NormalSQLParser::K_WITH) {
      setState(143);
      match(NormalSQLParser::K_WITH);
      setState(145);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 8, _ctx)) {
      case 1: {
        setState(144);
        match(NormalSQLParser::K_RECURSIVE);
        break;
      }

      }
      setState(147);
      common_table_expression();
      setState(152);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == NormalSQLParser::COMMA) {
        setState(148);
        match(NormalSQLParser::COMMA);
        setState(149);
        common_table_expression();
        setState(154);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(157);
    select_core();
    setState(163);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == NormalSQLParser::K_EXCEPT

    || _la == NormalSQLParser::K_INTERSECT || _la == NormalSQLParser::K_UNION) {
      setState(158);
      compound_operator();
      setState(159);
      select_core();
      setState(165);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(176);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NormalSQLParser::K_ORDER) {
      setState(166);
      match(NormalSQLParser::K_ORDER);
      setState(167);
      match(NormalSQLParser::K_BY);
      setState(168);
      ordering_term();
      setState(173);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == NormalSQLParser::COMMA) {
        setState(169);
        match(NormalSQLParser::COMMA);
        setState(170);
        ordering_term();
        setState(175);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(184);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NormalSQLParser::K_LIMIT) {
      setState(178);
      match(NormalSQLParser::K_LIMIT);
      setState(179);
      expr(0);
      setState(182);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NormalSQLParser::COMMA || _la == NormalSQLParser::K_OFFSET) {
        setState(180);
        _la = _input->LA(1);
        if (!(_la == NormalSQLParser::COMMA || _la == NormalSQLParser::K_OFFSET)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(181);
        expr(0);
      }
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Select_coreContext ------------------------------------------------------------------

NormalSQLParser::Select_coreContext::Select_coreContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::Select_coreContext::K_SELECT() {
  return getToken(NormalSQLParser::K_SELECT, 0);
}

std::vector<NormalSQLParser::Result_columnContext *> NormalSQLParser::Select_coreContext::result_column() {
  return getRuleContexts<NormalSQLParser::Result_columnContext>();
}

NormalSQLParser::Result_columnContext* NormalSQLParser::Select_coreContext::result_column(size_t i) {
  return getRuleContext<NormalSQLParser::Result_columnContext>(i);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Select_coreContext::COMMA() {
  return getTokens(NormalSQLParser::COMMA);
}

tree::TerminalNode* NormalSQLParser::Select_coreContext::COMMA(size_t i) {
  return getToken(NormalSQLParser::COMMA, i);
}

NormalSQLParser::From_clauseContext* NormalSQLParser::Select_coreContext::from_clause() {
  return getRuleContext<NormalSQLParser::From_clauseContext>(0);
}

NormalSQLParser::Where_clauseContext* NormalSQLParser::Select_coreContext::where_clause() {
  return getRuleContext<NormalSQLParser::Where_clauseContext>(0);
}

NormalSQLParser::GroupBy_clauseContext* NormalSQLParser::Select_coreContext::groupBy_clause() {
  return getRuleContext<NormalSQLParser::GroupBy_clauseContext>(0);
}

tree::TerminalNode* NormalSQLParser::Select_coreContext::K_DISTINCT() {
  return getToken(NormalSQLParser::K_DISTINCT, 0);
}

tree::TerminalNode* NormalSQLParser::Select_coreContext::K_ALL() {
  return getToken(NormalSQLParser::K_ALL, 0);
}

tree::TerminalNode* NormalSQLParser::Select_coreContext::K_VALUES() {
  return getToken(NormalSQLParser::K_VALUES, 0);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Select_coreContext::OPEN_PAR() {
  return getTokens(NormalSQLParser::OPEN_PAR);
}

tree::TerminalNode* NormalSQLParser::Select_coreContext::OPEN_PAR(size_t i) {
  return getToken(NormalSQLParser::OPEN_PAR, i);
}

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::Select_coreContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::Select_coreContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Select_coreContext::CLOSE_PAR() {
  return getTokens(NormalSQLParser::CLOSE_PAR);
}

tree::TerminalNode* NormalSQLParser::Select_coreContext::CLOSE_PAR(size_t i) {
  return getToken(NormalSQLParser::CLOSE_PAR, i);
}


size_t NormalSQLParser::Select_coreContext::getRuleIndex() const {
  return NormalSQLParser::RuleSelect_core;
}

void NormalSQLParser::Select_coreContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSelect_core(this);
}

void NormalSQLParser::Select_coreContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSelect_core(this);
}


antlrcpp::Any NormalSQLParser::Select_coreContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitSelect_core(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Select_coreContext* NormalSQLParser::select_core() {
  Select_coreContext *_localctx = _tracker.createInstance<Select_coreContext>(_ctx, getState());
  enterRule(_localctx, 10, NormalSQLParser::RuleSelect_core);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(235);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case NormalSQLParser::K_SELECT: {
        enterOuterAlt(_localctx, 1);
        setState(186);
        match(NormalSQLParser::K_SELECT);
        setState(188);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 16, _ctx)) {
        case 1: {
          setState(187);
          _la = _input->LA(1);
          if (!(_la == NormalSQLParser::K_ALL

          || _la == NormalSQLParser::K_DISTINCT)) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          break;
        }

        }
        setState(190);
        result_column();
        setState(195);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NormalSQLParser::COMMA) {
          setState(191);
          match(NormalSQLParser::COMMA);
          setState(192);
          result_column();
          setState(197);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(199);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NormalSQLParser::K_FROM) {
          setState(198);
          from_clause();
        }
        setState(202);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NormalSQLParser::K_WHERE) {
          setState(201);
          where_clause();
        }
        setState(205);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NormalSQLParser::K_GROUP) {
          setState(204);
          groupBy_clause();
        }
        break;
      }

      case NormalSQLParser::K_VALUES: {
        enterOuterAlt(_localctx, 2);
        setState(207);
        match(NormalSQLParser::K_VALUES);
        setState(208);
        match(NormalSQLParser::OPEN_PAR);
        setState(209);
        expr(0);
        setState(214);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NormalSQLParser::COMMA) {
          setState(210);
          match(NormalSQLParser::COMMA);
          setState(211);
          expr(0);
          setState(216);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(217);
        match(NormalSQLParser::CLOSE_PAR);
        setState(232);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NormalSQLParser::COMMA) {
          setState(218);
          match(NormalSQLParser::COMMA);
          setState(219);
          match(NormalSQLParser::OPEN_PAR);
          setState(220);
          expr(0);
          setState(225);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == NormalSQLParser::COMMA) {
            setState(221);
            match(NormalSQLParser::COMMA);
            setState(222);
            expr(0);
            setState(227);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
          setState(228);
          match(NormalSQLParser::CLOSE_PAR);
          setState(234);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- From_clauseContext ------------------------------------------------------------------

NormalSQLParser::From_clauseContext::From_clauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::From_clauseContext::K_FROM() {
  return getToken(NormalSQLParser::K_FROM, 0);
}

std::vector<NormalSQLParser::Table_or_subqueryContext *> NormalSQLParser::From_clauseContext::table_or_subquery() {
  return getRuleContexts<NormalSQLParser::Table_or_subqueryContext>();
}

NormalSQLParser::Table_or_subqueryContext* NormalSQLParser::From_clauseContext::table_or_subquery(size_t i) {
  return getRuleContext<NormalSQLParser::Table_or_subqueryContext>(i);
}

NormalSQLParser::Join_clauseContext* NormalSQLParser::From_clauseContext::join_clause() {
  return getRuleContext<NormalSQLParser::Join_clauseContext>(0);
}

std::vector<tree::TerminalNode *> NormalSQLParser::From_clauseContext::COMMA() {
  return getTokens(NormalSQLParser::COMMA);
}

tree::TerminalNode* NormalSQLParser::From_clauseContext::COMMA(size_t i) {
  return getToken(NormalSQLParser::COMMA, i);
}


size_t NormalSQLParser::From_clauseContext::getRuleIndex() const {
  return NormalSQLParser::RuleFrom_clause;
}

void NormalSQLParser::From_clauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFrom_clause(this);
}

void NormalSQLParser::From_clauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFrom_clause(this);
}


antlrcpp::Any NormalSQLParser::From_clauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitFrom_clause(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::From_clauseContext* NormalSQLParser::from_clause() {
  From_clauseContext *_localctx = _tracker.createInstance<From_clauseContext>(_ctx, getState());
  enterRule(_localctx, 12, NormalSQLParser::RuleFrom_clause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(237);
    match(NormalSQLParser::K_FROM);
    setState(247);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 26, _ctx)) {
    case 1: {
      setState(238);
      table_or_subquery();
      setState(243);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == NormalSQLParser::COMMA) {
        setState(239);
        match(NormalSQLParser::COMMA);
        setState(240);
        table_or_subquery();
        setState(245);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      break;
    }

    case 2: {
      setState(246);
      join_clause();
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Where_clauseContext ------------------------------------------------------------------

NormalSQLParser::Where_clauseContext::Where_clauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::Where_clauseContext::K_WHERE() {
  return getToken(NormalSQLParser::K_WHERE, 0);
}

NormalSQLParser::ExprContext* NormalSQLParser::Where_clauseContext::expr() {
  return getRuleContext<NormalSQLParser::ExprContext>(0);
}


size_t NormalSQLParser::Where_clauseContext::getRuleIndex() const {
  return NormalSQLParser::RuleWhere_clause;
}

void NormalSQLParser::Where_clauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterWhere_clause(this);
}

void NormalSQLParser::Where_clauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitWhere_clause(this);
}


antlrcpp::Any NormalSQLParser::Where_clauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitWhere_clause(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Where_clauseContext* NormalSQLParser::where_clause() {
  Where_clauseContext *_localctx = _tracker.createInstance<Where_clauseContext>(_ctx, getState());
  enterRule(_localctx, 14, NormalSQLParser::RuleWhere_clause);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(249);
    match(NormalSQLParser::K_WHERE);
    setState(250);
    expr(0);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupBy_clauseContext ------------------------------------------------------------------

NormalSQLParser::GroupBy_clauseContext::GroupBy_clauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::GroupBy_clauseContext::K_GROUP() {
  return getToken(NormalSQLParser::K_GROUP, 0);
}

tree::TerminalNode* NormalSQLParser::GroupBy_clauseContext::K_BY() {
  return getToken(NormalSQLParser::K_BY, 0);
}

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::GroupBy_clauseContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::GroupBy_clauseContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

std::vector<tree::TerminalNode *> NormalSQLParser::GroupBy_clauseContext::COMMA() {
  return getTokens(NormalSQLParser::COMMA);
}

tree::TerminalNode* NormalSQLParser::GroupBy_clauseContext::COMMA(size_t i) {
  return getToken(NormalSQLParser::COMMA, i);
}

tree::TerminalNode* NormalSQLParser::GroupBy_clauseContext::K_HAVING() {
  return getToken(NormalSQLParser::K_HAVING, 0);
}


size_t NormalSQLParser::GroupBy_clauseContext::getRuleIndex() const {
  return NormalSQLParser::RuleGroupBy_clause;
}

void NormalSQLParser::GroupBy_clauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupBy_clause(this);
}

void NormalSQLParser::GroupBy_clauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGroupBy_clause(this);
}


antlrcpp::Any NormalSQLParser::GroupBy_clauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitGroupBy_clause(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::GroupBy_clauseContext* NormalSQLParser::groupBy_clause() {
  GroupBy_clauseContext *_localctx = _tracker.createInstance<GroupBy_clauseContext>(_ctx, getState());
  enterRule(_localctx, 16, NormalSQLParser::RuleGroupBy_clause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(252);
    match(NormalSQLParser::K_GROUP);
    setState(253);
    match(NormalSQLParser::K_BY);
    setState(254);
    expr(0);
    setState(259);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == NormalSQLParser::COMMA) {
      setState(255);
      match(NormalSQLParser::COMMA);
      setState(256);
      expr(0);
      setState(261);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(264);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NormalSQLParser::K_HAVING) {
      setState(262);
      match(NormalSQLParser::K_HAVING);
      setState(263);
      expr(0);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Type_nameContext ------------------------------------------------------------------

NormalSQLParser::Type_nameContext::Type_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<NormalSQLParser::NameContext *> NormalSQLParser::Type_nameContext::name() {
  return getRuleContexts<NormalSQLParser::NameContext>();
}

NormalSQLParser::NameContext* NormalSQLParser::Type_nameContext::name(size_t i) {
  return getRuleContext<NormalSQLParser::NameContext>(i);
}

tree::TerminalNode* NormalSQLParser::Type_nameContext::OPEN_PAR() {
  return getToken(NormalSQLParser::OPEN_PAR, 0);
}

std::vector<NormalSQLParser::Signed_numberContext *> NormalSQLParser::Type_nameContext::signed_number() {
  return getRuleContexts<NormalSQLParser::Signed_numberContext>();
}

NormalSQLParser::Signed_numberContext* NormalSQLParser::Type_nameContext::signed_number(size_t i) {
  return getRuleContext<NormalSQLParser::Signed_numberContext>(i);
}

tree::TerminalNode* NormalSQLParser::Type_nameContext::CLOSE_PAR() {
  return getToken(NormalSQLParser::CLOSE_PAR, 0);
}

tree::TerminalNode* NormalSQLParser::Type_nameContext::COMMA() {
  return getToken(NormalSQLParser::COMMA, 0);
}


size_t NormalSQLParser::Type_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleType_name;
}

void NormalSQLParser::Type_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterType_name(this);
}

void NormalSQLParser::Type_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitType_name(this);
}


antlrcpp::Any NormalSQLParser::Type_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitType_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Type_nameContext* NormalSQLParser::type_name() {
  Type_nameContext *_localctx = _tracker.createInstance<Type_nameContext>(_ctx, getState());
  enterRule(_localctx, 18, NormalSQLParser::RuleType_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(267); 
    _errHandler->sync(this);
    alt = 1;
    do {
      switch (alt) {
        case 1: {
              setState(266);
              name();
              break;
            }

      default:
        throw NoViableAltException(this);
      }
      setState(269); 
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 29, _ctx);
    } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);
    setState(281);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 30, _ctx)) {
    case 1: {
      setState(271);
      match(NormalSQLParser::OPEN_PAR);
      setState(272);
      signed_number();
      setState(273);
      match(NormalSQLParser::CLOSE_PAR);
      break;
    }

    case 2: {
      setState(275);
      match(NormalSQLParser::OPEN_PAR);
      setState(276);
      signed_number();
      setState(277);
      match(NormalSQLParser::COMMA);
      setState(278);
      signed_number();
      setState(279);
      match(NormalSQLParser::CLOSE_PAR);
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExprContext ------------------------------------------------------------------

NormalSQLParser::ExprContext::ExprContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NormalSQLParser::ExprContext::getRuleIndex() const {
  return NormalSQLParser::RuleExpr;
}

void NormalSQLParser::ExprContext::copyFrom(ExprContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- Expr_compContext ------------------------------------------------------------------

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::Expr_compContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_compContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

tree::TerminalNode* NormalSQLParser::Expr_compContext::LT() {
  return getToken(NormalSQLParser::LT, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_compContext::LT_EQ() {
  return getToken(NormalSQLParser::LT_EQ, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_compContext::GT() {
  return getToken(NormalSQLParser::GT, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_compContext::GT_EQ() {
  return getToken(NormalSQLParser::GT_EQ, 0);
}

NormalSQLParser::Expr_compContext::Expr_compContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_compContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_comp(this);
}
void NormalSQLParser::Expr_compContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_comp(this);
}

antlrcpp::Any NormalSQLParser::Expr_compContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_comp(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_caseContext ------------------------------------------------------------------

tree::TerminalNode* NormalSQLParser::Expr_caseContext::K_CASE() {
  return getToken(NormalSQLParser::K_CASE, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_caseContext::K_END() {
  return getToken(NormalSQLParser::K_END, 0);
}

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::Expr_caseContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_caseContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Expr_caseContext::K_WHEN() {
  return getTokens(NormalSQLParser::K_WHEN);
}

tree::TerminalNode* NormalSQLParser::Expr_caseContext::K_WHEN(size_t i) {
  return getToken(NormalSQLParser::K_WHEN, i);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Expr_caseContext::K_THEN() {
  return getTokens(NormalSQLParser::K_THEN);
}

tree::TerminalNode* NormalSQLParser::Expr_caseContext::K_THEN(size_t i) {
  return getToken(NormalSQLParser::K_THEN, i);
}

tree::TerminalNode* NormalSQLParser::Expr_caseContext::K_ELSE() {
  return getToken(NormalSQLParser::K_ELSE, 0);
}

NormalSQLParser::Expr_caseContext::Expr_caseContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_caseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_case(this);
}
void NormalSQLParser::Expr_caseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_case(this);
}

antlrcpp::Any NormalSQLParser::Expr_caseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_case(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_raise_functionContext ------------------------------------------------------------------

NormalSQLParser::Raise_functionContext* NormalSQLParser::Expr_raise_functionContext::raise_function() {
  return getRuleContext<NormalSQLParser::Raise_functionContext>(0);
}

NormalSQLParser::Expr_raise_functionContext::Expr_raise_functionContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_raise_functionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_raise_function(this);
}
void NormalSQLParser::Expr_raise_functionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_raise_function(this);
}

antlrcpp::Any NormalSQLParser::Expr_raise_functionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_raise_function(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_parensContext ------------------------------------------------------------------

tree::TerminalNode* NormalSQLParser::Expr_parensContext::OPEN_PAR() {
  return getToken(NormalSQLParser::OPEN_PAR, 0);
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_parensContext::expr() {
  return getRuleContext<NormalSQLParser::ExprContext>(0);
}

tree::TerminalNode* NormalSQLParser::Expr_parensContext::CLOSE_PAR() {
  return getToken(NormalSQLParser::CLOSE_PAR, 0);
}

NormalSQLParser::Expr_parensContext::Expr_parensContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_parensContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_parens(this);
}
void NormalSQLParser::Expr_parensContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_parens(this);
}

antlrcpp::Any NormalSQLParser::Expr_parensContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_parens(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_columnContext ------------------------------------------------------------------

NormalSQLParser::Column_nameContext* NormalSQLParser::Expr_columnContext::column_name() {
  return getRuleContext<NormalSQLParser::Column_nameContext>(0);
}

NormalSQLParser::Table_nameContext* NormalSQLParser::Expr_columnContext::table_name() {
  return getRuleContext<NormalSQLParser::Table_nameContext>(0);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Expr_columnContext::DOT() {
  return getTokens(NormalSQLParser::DOT);
}

tree::TerminalNode* NormalSQLParser::Expr_columnContext::DOT(size_t i) {
  return getToken(NormalSQLParser::DOT, i);
}

NormalSQLParser::Database_nameContext* NormalSQLParser::Expr_columnContext::database_name() {
  return getRuleContext<NormalSQLParser::Database_nameContext>(0);
}

NormalSQLParser::Expr_columnContext::Expr_columnContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_columnContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_column(this);
}
void NormalSQLParser::Expr_columnContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_column(this);
}

antlrcpp::Any NormalSQLParser::Expr_columnContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_column(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_functionContext ------------------------------------------------------------------

NormalSQLParser::Function_nameContext* NormalSQLParser::Expr_functionContext::function_name() {
  return getRuleContext<NormalSQLParser::Function_nameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Expr_functionContext::OPEN_PAR() {
  return getToken(NormalSQLParser::OPEN_PAR, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_functionContext::CLOSE_PAR() {
  return getToken(NormalSQLParser::CLOSE_PAR, 0);
}

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::Expr_functionContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_functionContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

tree::TerminalNode* NormalSQLParser::Expr_functionContext::STAR() {
  return getToken(NormalSQLParser::STAR, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_functionContext::K_DISTINCT() {
  return getToken(NormalSQLParser::K_DISTINCT, 0);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Expr_functionContext::COMMA() {
  return getTokens(NormalSQLParser::COMMA);
}

tree::TerminalNode* NormalSQLParser::Expr_functionContext::COMMA(size_t i) {
  return getToken(NormalSQLParser::COMMA, i);
}

NormalSQLParser::Expr_functionContext::Expr_functionContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_functionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_function(this);
}
void NormalSQLParser::Expr_functionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_function(this);
}

antlrcpp::Any NormalSQLParser::Expr_functionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_function(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_eqContext ------------------------------------------------------------------

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::Expr_eqContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_eqContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

tree::TerminalNode* NormalSQLParser::Expr_eqContext::ASSIGN() {
  return getToken(NormalSQLParser::ASSIGN, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_eqContext::EQ() {
  return getToken(NormalSQLParser::EQ, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_eqContext::NOT_EQ1() {
  return getToken(NormalSQLParser::NOT_EQ1, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_eqContext::NOT_EQ2() {
  return getToken(NormalSQLParser::NOT_EQ2, 0);
}

NormalSQLParser::Expr_eqContext::Expr_eqContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_eqContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_eq(this);
}
void NormalSQLParser::Expr_eqContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_eq(this);
}

antlrcpp::Any NormalSQLParser::Expr_eqContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_eq(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_nullContext ------------------------------------------------------------------

NormalSQLParser::ExprContext* NormalSQLParser::Expr_nullContext::expr() {
  return getRuleContext<NormalSQLParser::ExprContext>(0);
}

tree::TerminalNode* NormalSQLParser::Expr_nullContext::K_ISNULL() {
  return getToken(NormalSQLParser::K_ISNULL, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_nullContext::K_NOTNULL() {
  return getToken(NormalSQLParser::K_NOTNULL, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_nullContext::K_NOT() {
  return getToken(NormalSQLParser::K_NOT, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_nullContext::K_NULL() {
  return getToken(NormalSQLParser::K_NULL, 0);
}

NormalSQLParser::Expr_nullContext::Expr_nullContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_nullContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_null(this);
}
void NormalSQLParser::Expr_nullContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_null(this);
}

antlrcpp::Any NormalSQLParser::Expr_nullContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_null(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_bind_parameterContext ------------------------------------------------------------------

tree::TerminalNode* NormalSQLParser::Expr_bind_parameterContext::BIND_PARAMETER() {
  return getToken(NormalSQLParser::BIND_PARAMETER, 0);
}

NormalSQLParser::Expr_bind_parameterContext::Expr_bind_parameterContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_bind_parameterContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_bind_parameter(this);
}
void NormalSQLParser::Expr_bind_parameterContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_bind_parameter(this);
}

antlrcpp::Any NormalSQLParser::Expr_bind_parameterContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_bind_parameter(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_isContext ------------------------------------------------------------------

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::Expr_isContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_isContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

tree::TerminalNode* NormalSQLParser::Expr_isContext::K_IS() {
  return getToken(NormalSQLParser::K_IS, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_isContext::K_NOT() {
  return getToken(NormalSQLParser::K_NOT, 0);
}

NormalSQLParser::Expr_isContext::Expr_isContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_isContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_is(this);
}
void NormalSQLParser::Expr_isContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_is(this);
}

antlrcpp::Any NormalSQLParser::Expr_isContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_is(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_literalContext ------------------------------------------------------------------

NormalSQLParser::Literal_valueContext* NormalSQLParser::Expr_literalContext::literal_value() {
  return getRuleContext<NormalSQLParser::Literal_valueContext>(0);
}

NormalSQLParser::Expr_literalContext::Expr_literalContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_literalContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_literal(this);
}
void NormalSQLParser::Expr_literalContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_literal(this);
}

antlrcpp::Any NormalSQLParser::Expr_literalContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_literal(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_castContext ------------------------------------------------------------------

tree::TerminalNode* NormalSQLParser::Expr_castContext::K_CAST() {
  return getToken(NormalSQLParser::K_CAST, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_castContext::OPEN_PAR() {
  return getToken(NormalSQLParser::OPEN_PAR, 0);
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_castContext::expr() {
  return getRuleContext<NormalSQLParser::ExprContext>(0);
}

tree::TerminalNode* NormalSQLParser::Expr_castContext::K_AS() {
  return getToken(NormalSQLParser::K_AS, 0);
}

NormalSQLParser::Type_nameContext* NormalSQLParser::Expr_castContext::type_name() {
  return getRuleContext<NormalSQLParser::Type_nameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Expr_castContext::CLOSE_PAR() {
  return getToken(NormalSQLParser::CLOSE_PAR, 0);
}

NormalSQLParser::Expr_castContext::Expr_castContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_castContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_cast(this);
}
void NormalSQLParser::Expr_castContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_cast(this);
}

antlrcpp::Any NormalSQLParser::Expr_castContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_cast(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_existsContext ------------------------------------------------------------------

tree::TerminalNode* NormalSQLParser::Expr_existsContext::OPEN_PAR() {
  return getToken(NormalSQLParser::OPEN_PAR, 0);
}

NormalSQLParser::Select_stmtContext* NormalSQLParser::Expr_existsContext::select_stmt() {
  return getRuleContext<NormalSQLParser::Select_stmtContext>(0);
}

tree::TerminalNode* NormalSQLParser::Expr_existsContext::CLOSE_PAR() {
  return getToken(NormalSQLParser::CLOSE_PAR, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_existsContext::K_EXISTS() {
  return getToken(NormalSQLParser::K_EXISTS, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_existsContext::K_NOT() {
  return getToken(NormalSQLParser::K_NOT, 0);
}

NormalSQLParser::Expr_existsContext::Expr_existsContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_existsContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_exists(this);
}
void NormalSQLParser::Expr_existsContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_exists(this);
}

antlrcpp::Any NormalSQLParser::Expr_existsContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_exists(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_mul_div_modContext ------------------------------------------------------------------

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::Expr_mul_div_modContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_mul_div_modContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

tree::TerminalNode* NormalSQLParser::Expr_mul_div_modContext::STAR() {
  return getToken(NormalSQLParser::STAR, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_mul_div_modContext::DIV() {
  return getToken(NormalSQLParser::DIV, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_mul_div_modContext::MOD() {
  return getToken(NormalSQLParser::MOD, 0);
}

NormalSQLParser::Expr_mul_div_modContext::Expr_mul_div_modContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_mul_div_modContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_mul_div_mod(this);
}
void NormalSQLParser::Expr_mul_div_modContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_mul_div_mod(this);
}

antlrcpp::Any NormalSQLParser::Expr_mul_div_modContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_mul_div_mod(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_betweenContext ------------------------------------------------------------------

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::Expr_betweenContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_betweenContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

tree::TerminalNode* NormalSQLParser::Expr_betweenContext::K_BETWEEN() {
  return getToken(NormalSQLParser::K_BETWEEN, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_betweenContext::K_AND() {
  return getToken(NormalSQLParser::K_AND, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_betweenContext::K_NOT() {
  return getToken(NormalSQLParser::K_NOT, 0);
}

NormalSQLParser::Expr_betweenContext::Expr_betweenContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_betweenContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_between(this);
}
void NormalSQLParser::Expr_betweenContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_between(this);
}

antlrcpp::Any NormalSQLParser::Expr_betweenContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_between(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_inContext ------------------------------------------------------------------

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::Expr_inContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_inContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

tree::TerminalNode* NormalSQLParser::Expr_inContext::K_IN() {
  return getToken(NormalSQLParser::K_IN, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_inContext::OPEN_PAR() {
  return getToken(NormalSQLParser::OPEN_PAR, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_inContext::CLOSE_PAR() {
  return getToken(NormalSQLParser::CLOSE_PAR, 0);
}

NormalSQLParser::Table_nameContext* NormalSQLParser::Expr_inContext::table_name() {
  return getRuleContext<NormalSQLParser::Table_nameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Expr_inContext::K_NOT() {
  return getToken(NormalSQLParser::K_NOT, 0);
}

NormalSQLParser::Select_stmtContext* NormalSQLParser::Expr_inContext::select_stmt() {
  return getRuleContext<NormalSQLParser::Select_stmtContext>(0);
}

NormalSQLParser::Database_nameContext* NormalSQLParser::Expr_inContext::database_name() {
  return getRuleContext<NormalSQLParser::Database_nameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Expr_inContext::DOT() {
  return getToken(NormalSQLParser::DOT, 0);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Expr_inContext::COMMA() {
  return getTokens(NormalSQLParser::COMMA);
}

tree::TerminalNode* NormalSQLParser::Expr_inContext::COMMA(size_t i) {
  return getToken(NormalSQLParser::COMMA, i);
}

NormalSQLParser::Expr_inContext::Expr_inContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_inContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_in(this);
}
void NormalSQLParser::Expr_inContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_in(this);
}

antlrcpp::Any NormalSQLParser::Expr_inContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_in(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_bitContext ------------------------------------------------------------------

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::Expr_bitContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_bitContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

tree::TerminalNode* NormalSQLParser::Expr_bitContext::LT2() {
  return getToken(NormalSQLParser::LT2, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_bitContext::GT2() {
  return getToken(NormalSQLParser::GT2, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_bitContext::AMP() {
  return getToken(NormalSQLParser::AMP, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_bitContext::PIPE() {
  return getToken(NormalSQLParser::PIPE, 0);
}

NormalSQLParser::Expr_bitContext::Expr_bitContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_bitContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_bit(this);
}
void NormalSQLParser::Expr_bitContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_bit(this);
}

antlrcpp::Any NormalSQLParser::Expr_bitContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_bit(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_orContext ------------------------------------------------------------------

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::Expr_orContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_orContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

tree::TerminalNode* NormalSQLParser::Expr_orContext::K_OR() {
  return getToken(NormalSQLParser::K_OR, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_orContext::PIPE2() {
  return getToken(NormalSQLParser::PIPE2, 0);
}

NormalSQLParser::Expr_orContext::Expr_orContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_orContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_or(this);
}
void NormalSQLParser::Expr_orContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_or(this);
}

antlrcpp::Any NormalSQLParser::Expr_orContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_or(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_collateContext ------------------------------------------------------------------

NormalSQLParser::ExprContext* NormalSQLParser::Expr_collateContext::expr() {
  return getRuleContext<NormalSQLParser::ExprContext>(0);
}

tree::TerminalNode* NormalSQLParser::Expr_collateContext::K_COLLATE() {
  return getToken(NormalSQLParser::K_COLLATE, 0);
}

NormalSQLParser::Collation_nameContext* NormalSQLParser::Expr_collateContext::collation_name() {
  return getRuleContext<NormalSQLParser::Collation_nameContext>(0);
}

NormalSQLParser::Expr_collateContext::Expr_collateContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_collateContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_collate(this);
}
void NormalSQLParser::Expr_collateContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_collate(this);
}

antlrcpp::Any NormalSQLParser::Expr_collateContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_collate(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_andContext ------------------------------------------------------------------

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::Expr_andContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_andContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

tree::TerminalNode* NormalSQLParser::Expr_andContext::K_AND() {
  return getToken(NormalSQLParser::K_AND, 0);
}

NormalSQLParser::Expr_andContext::Expr_andContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_andContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_and(this);
}
void NormalSQLParser::Expr_andContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_and(this);
}

antlrcpp::Any NormalSQLParser::Expr_andContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_and(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_add_subContext ------------------------------------------------------------------

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::Expr_add_subContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_add_subContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

tree::TerminalNode* NormalSQLParser::Expr_add_subContext::PLUS() {
  return getToken(NormalSQLParser::PLUS, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_add_subContext::MINUS() {
  return getToken(NormalSQLParser::MINUS, 0);
}

NormalSQLParser::Expr_add_subContext::Expr_add_subContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_add_subContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_add_sub(this);
}
void NormalSQLParser::Expr_add_subContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_add_sub(this);
}

antlrcpp::Any NormalSQLParser::Expr_add_subContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_add_sub(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_unaryContext ------------------------------------------------------------------

NormalSQLParser::Unary_operatorContext* NormalSQLParser::Expr_unaryContext::unary_operator() {
  return getRuleContext<NormalSQLParser::Unary_operatorContext>(0);
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_unaryContext::expr() {
  return getRuleContext<NormalSQLParser::ExprContext>(0);
}

NormalSQLParser::Expr_unaryContext::Expr_unaryContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_unaryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_unary(this);
}
void NormalSQLParser::Expr_unaryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_unary(this);
}

antlrcpp::Any NormalSQLParser::Expr_unaryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_unary(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Expr_str_matchContext ------------------------------------------------------------------

std::vector<NormalSQLParser::ExprContext *> NormalSQLParser::Expr_str_matchContext::expr() {
  return getRuleContexts<NormalSQLParser::ExprContext>();
}

NormalSQLParser::ExprContext* NormalSQLParser::Expr_str_matchContext::expr(size_t i) {
  return getRuleContext<NormalSQLParser::ExprContext>(i);
}

tree::TerminalNode* NormalSQLParser::Expr_str_matchContext::K_LIKE() {
  return getToken(NormalSQLParser::K_LIKE, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_str_matchContext::K_GLOB() {
  return getToken(NormalSQLParser::K_GLOB, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_str_matchContext::K_REGEXP() {
  return getToken(NormalSQLParser::K_REGEXP, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_str_matchContext::K_MATCH() {
  return getToken(NormalSQLParser::K_MATCH, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_str_matchContext::K_NOT() {
  return getToken(NormalSQLParser::K_NOT, 0);
}

tree::TerminalNode* NormalSQLParser::Expr_str_matchContext::K_ESCAPE() {
  return getToken(NormalSQLParser::K_ESCAPE, 0);
}

NormalSQLParser::Expr_str_matchContext::Expr_str_matchContext(ExprContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Expr_str_matchContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpr_str_match(this);
}
void NormalSQLParser::Expr_str_matchContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpr_str_match(this);
}

antlrcpp::Any NormalSQLParser::Expr_str_matchContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitExpr_str_match(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::ExprContext* NormalSQLParser::expr() {
   return expr(0);
}

NormalSQLParser::ExprContext* NormalSQLParser::expr(int precedence) {
  ParserRuleContext *parentContext = _ctx;
  size_t parentState = getState();
  NormalSQLParser::ExprContext *_localctx = _tracker.createInstance<ExprContext>(_ctx, parentState);
  NormalSQLParser::ExprContext *previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by generated code.
  size_t startState = 20;
  enterRecursionRule(_localctx, 20, NormalSQLParser::RuleExpr, precedence);

    size_t _la = 0;

  auto onExit = finally([=] {
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(359);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 41, _ctx)) {
    case 1: {
      _localctx = _tracker.createInstance<Expr_literalContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;

      setState(284);
      literal_value();
      break;
    }

    case 2: {
      _localctx = _tracker.createInstance<Expr_bind_parameterContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(285);
      match(NormalSQLParser::BIND_PARAMETER);
      break;
    }

    case 3: {
      _localctx = _tracker.createInstance<Expr_columnContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(294);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 32, _ctx)) {
      case 1: {
        setState(289);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 31, _ctx)) {
        case 1: {
          setState(286);
          database_name();
          setState(287);
          match(NormalSQLParser::DOT);
          break;
        }

        }
        setState(291);
        table_name();
        setState(292);
        match(NormalSQLParser::DOT);
        break;
      }

      }
      setState(296);
      column_name();
      break;
    }

    case 4: {
      _localctx = _tracker.createInstance<Expr_unaryContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(297);
      unary_operator();
      setState(298);
      expr(22);
      break;
    }

    case 5: {
      _localctx = _tracker.createInstance<Expr_functionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(300);
      function_name();
      setState(301);
      match(NormalSQLParser::OPEN_PAR);
      setState(314);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case NormalSQLParser::OPEN_PAR:
        case NormalSQLParser::PLUS:
        case NormalSQLParser::MINUS:
        case NormalSQLParser::TILDE:
        case NormalSQLParser::K_ABORT:
        case NormalSQLParser::K_ACTION:
        case NormalSQLParser::K_ADD:
        case NormalSQLParser::K_AFTER:
        case NormalSQLParser::K_ALL:
        case NormalSQLParser::K_ALTER:
        case NormalSQLParser::K_ANALYZE:
        case NormalSQLParser::K_AND:
        case NormalSQLParser::K_AS:
        case NormalSQLParser::K_ASC:
        case NormalSQLParser::K_ATTACH:
        case NormalSQLParser::K_AUTOINCREMENT:
        case NormalSQLParser::K_BEFORE:
        case NormalSQLParser::K_BEGIN:
        case NormalSQLParser::K_BETWEEN:
        case NormalSQLParser::K_BY:
        case NormalSQLParser::K_CASCADE:
        case NormalSQLParser::K_CASE:
        case NormalSQLParser::K_CAST:
        case NormalSQLParser::K_CHECK:
        case NormalSQLParser::K_COLLATE:
        case NormalSQLParser::K_COLUMN:
        case NormalSQLParser::K_COMMIT:
        case NormalSQLParser::K_CONFLICT:
        case NormalSQLParser::K_CONSTRAINT:
        case NormalSQLParser::K_CREATE:
        case NormalSQLParser::K_CROSS:
        case NormalSQLParser::K_CURRENT_DATE:
        case NormalSQLParser::K_CURRENT_TIME:
        case NormalSQLParser::K_CURRENT_TIMESTAMP:
        case NormalSQLParser::K_DATABASE:
        case NormalSQLParser::K_DEFAULT:
        case NormalSQLParser::K_DEFERRABLE:
        case NormalSQLParser::K_DEFERRED:
        case NormalSQLParser::K_DELETE:
        case NormalSQLParser::K_DESC:
        case NormalSQLParser::K_DETACH:
        case NormalSQLParser::K_DISTINCT:
        case NormalSQLParser::K_DROP:
        case NormalSQLParser::K_EACH:
        case NormalSQLParser::K_ELSE:
        case NormalSQLParser::K_END:
        case NormalSQLParser::K_ESCAPE:
        case NormalSQLParser::K_EXCEPT:
        case NormalSQLParser::K_EXCLUSIVE:
        case NormalSQLParser::K_EXISTS:
        case NormalSQLParser::K_EXPLAIN:
        case NormalSQLParser::K_FAIL:
        case NormalSQLParser::K_FOR:
        case NormalSQLParser::K_FOREIGN:
        case NormalSQLParser::K_FROM:
        case NormalSQLParser::K_FULL:
        case NormalSQLParser::K_GLOB:
        case NormalSQLParser::K_GROUP:
        case NormalSQLParser::K_HAVING:
        case NormalSQLParser::K_IF:
        case NormalSQLParser::K_IGNORE:
        case NormalSQLParser::K_IMMEDIATE:
        case NormalSQLParser::K_IN:
        case NormalSQLParser::K_INDEX:
        case NormalSQLParser::K_INDEXED:
        case NormalSQLParser::K_INITIALLY:
        case NormalSQLParser::K_INNER:
        case NormalSQLParser::K_INSERT:
        case NormalSQLParser::K_INSTEAD:
        case NormalSQLParser::K_INTERSECT:
        case NormalSQLParser::K_INTO:
        case NormalSQLParser::K_IS:
        case NormalSQLParser::K_ISNULL:
        case NormalSQLParser::K_JOIN:
        case NormalSQLParser::K_KEY:
        case NormalSQLParser::K_LEFT:
        case NormalSQLParser::K_LIKE:
        case NormalSQLParser::K_LIMIT:
        case NormalSQLParser::K_MATCH:
        case NormalSQLParser::K_NATURAL:
        case NormalSQLParser::K_NO:
        case NormalSQLParser::K_NOT:
        case NormalSQLParser::K_NOTNULL:
        case NormalSQLParser::K_NULL:
        case NormalSQLParser::K_OF:
        case NormalSQLParser::K_OFFSET:
        case NormalSQLParser::K_ON:
        case NormalSQLParser::K_OR:
        case NormalSQLParser::K_ORDER:
        case NormalSQLParser::K_OUTER:
        case NormalSQLParser::K_PLAN:
        case NormalSQLParser::K_PRAGMA:
        case NormalSQLParser::K_PRIMARY:
        case NormalSQLParser::K_QUERY:
        case NormalSQLParser::K_RAISE:
        case NormalSQLParser::K_RECURSIVE:
        case NormalSQLParser::K_REFERENCES:
        case NormalSQLParser::K_REGEXP:
        case NormalSQLParser::K_REINDEX:
        case NormalSQLParser::K_RELEASE:
        case NormalSQLParser::K_RENAME:
        case NormalSQLParser::K_REPLACE:
        case NormalSQLParser::K_RESTRICT:
        case NormalSQLParser::K_RIGHT:
        case NormalSQLParser::K_ROLLBACK:
        case NormalSQLParser::K_ROW:
        case NormalSQLParser::K_SAVEPOINT:
        case NormalSQLParser::K_SELECT:
        case NormalSQLParser::K_SET:
        case NormalSQLParser::K_TABLE:
        case NormalSQLParser::K_TEMP:
        case NormalSQLParser::K_TEMPORARY:
        case NormalSQLParser::K_THEN:
        case NormalSQLParser::K_TO:
        case NormalSQLParser::K_TRANSACTION:
        case NormalSQLParser::K_TRIGGER:
        case NormalSQLParser::K_UNION:
        case NormalSQLParser::K_UNIQUE:
        case NormalSQLParser::K_UPDATE:
        case NormalSQLParser::K_USING:
        case NormalSQLParser::K_VACUUM:
        case NormalSQLParser::K_VALUES:
        case NormalSQLParser::K_VIEW:
        case NormalSQLParser::K_VIRTUAL:
        case NormalSQLParser::K_WHEN:
        case NormalSQLParser::K_WHERE:
        case NormalSQLParser::K_WITH:
        case NormalSQLParser::K_WITHOUT:
        case NormalSQLParser::IDENTIFIER:
        case NormalSQLParser::NUMERIC_LITERAL:
        case NormalSQLParser::BIND_PARAMETER:
        case NormalSQLParser::STRING_LITERAL:
        case NormalSQLParser::BLOB_LITERAL: {
          setState(303);
          _errHandler->sync(this);

          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 33, _ctx)) {
          case 1: {
            setState(302);
            match(NormalSQLParser::K_DISTINCT);
            break;
          }

          }
          setState(305);
          expr(0);
          setState(310);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == NormalSQLParser::COMMA) {
            setState(306);
            match(NormalSQLParser::COMMA);
            setState(307);
            expr(0);
            setState(312);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
          break;
        }

        case NormalSQLParser::STAR: {
          setState(313);
          match(NormalSQLParser::STAR);
          break;
        }

        case NormalSQLParser::CLOSE_PAR: {
          break;
        }

      default:
        break;
      }
      setState(316);
      match(NormalSQLParser::CLOSE_PAR);
      break;
    }

    case 6: {
      _localctx = _tracker.createInstance<Expr_parensContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(318);
      match(NormalSQLParser::OPEN_PAR);
      setState(319);
      expr(0);
      setState(320);
      match(NormalSQLParser::CLOSE_PAR);
      break;
    }

    case 7: {
      _localctx = _tracker.createInstance<Expr_castContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(322);
      match(NormalSQLParser::K_CAST);
      setState(323);
      match(NormalSQLParser::OPEN_PAR);
      setState(324);
      expr(0);
      setState(325);
      match(NormalSQLParser::K_AS);
      setState(326);
      type_name();
      setState(327);
      match(NormalSQLParser::CLOSE_PAR);
      break;
    }

    case 8: {
      _localctx = _tracker.createInstance<Expr_existsContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(333);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NormalSQLParser::K_EXISTS

      || _la == NormalSQLParser::K_NOT) {
        setState(330);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NormalSQLParser::K_NOT) {
          setState(329);
          match(NormalSQLParser::K_NOT);
        }
        setState(332);
        match(NormalSQLParser::K_EXISTS);
      }
      setState(335);
      match(NormalSQLParser::OPEN_PAR);
      setState(336);
      select_stmt();
      setState(337);
      match(NormalSQLParser::CLOSE_PAR);
      break;
    }

    case 9: {
      _localctx = _tracker.createInstance<Expr_caseContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(339);
      match(NormalSQLParser::K_CASE);
      setState(341);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 38, _ctx)) {
      case 1: {
        setState(340);
        expr(0);
        break;
      }

      }
      setState(348); 
      _errHandler->sync(this);
      _la = _input->LA(1);
      do {
        setState(343);
        match(NormalSQLParser::K_WHEN);
        setState(344);
        expr(0);
        setState(345);
        match(NormalSQLParser::K_THEN);
        setState(346);
        expr(0);
        setState(350); 
        _errHandler->sync(this);
        _la = _input->LA(1);
      } while (_la == NormalSQLParser::K_WHEN);
      setState(354);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NormalSQLParser::K_ELSE) {
        setState(352);
        match(NormalSQLParser::K_ELSE);
        setState(353);
        expr(0);
      }
      setState(356);
      match(NormalSQLParser::K_END);
      break;
    }

    case 10: {
      _localctx = _tracker.createInstance<Expr_raise_functionContext>(_localctx);
      _ctx = _localctx;
      previousContext = _localctx;
      setState(358);
      raise_function();
      break;
    }

    }
    _ctx->stop = _input->LT(-1);
    setState(451);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 53, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(449);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 52, _ctx)) {
        case 1: {
          auto newContext = _tracker.createInstance<Expr_mul_div_modContext>(_tracker.createInstance<ExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleExpr);
          setState(361);

          if (!(precpred(_ctx, 21))) throw FailedPredicateException(this, "precpred(_ctx, 21)");
          setState(362);
          _la = _input->LA(1);
          if (!((((_la & ~ 0x3fULL) == 0) &&
            ((1ULL << _la) & ((1ULL << NormalSQLParser::STAR)
            | (1ULL << NormalSQLParser::DIV)
            | (1ULL << NormalSQLParser::MOD))) != 0))) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(363);
          expr(22);
          break;
        }

        case 2: {
          auto newContext = _tracker.createInstance<Expr_add_subContext>(_tracker.createInstance<ExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleExpr);
          setState(364);

          if (!(precpred(_ctx, 20))) throw FailedPredicateException(this, "precpred(_ctx, 20)");
          setState(365);
          _la = _input->LA(1);
          if (!(_la == NormalSQLParser::PLUS

          || _la == NormalSQLParser::MINUS)) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(366);
          expr(21);
          break;
        }

        case 3: {
          auto newContext = _tracker.createInstance<Expr_bitContext>(_tracker.createInstance<ExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleExpr);
          setState(367);

          if (!(precpred(_ctx, 19))) throw FailedPredicateException(this, "precpred(_ctx, 19)");
          setState(368);
          _la = _input->LA(1);
          if (!((((_la & ~ 0x3fULL) == 0) &&
            ((1ULL << _la) & ((1ULL << NormalSQLParser::LT2)
            | (1ULL << NormalSQLParser::GT2)
            | (1ULL << NormalSQLParser::AMP)
            | (1ULL << NormalSQLParser::PIPE))) != 0))) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(369);
          expr(20);
          break;
        }

        case 4: {
          auto newContext = _tracker.createInstance<Expr_compContext>(_tracker.createInstance<ExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleExpr);
          setState(370);

          if (!(precpred(_ctx, 18))) throw FailedPredicateException(this, "precpred(_ctx, 18)");
          setState(371);
          _la = _input->LA(1);
          if (!((((_la & ~ 0x3fULL) == 0) &&
            ((1ULL << _la) & ((1ULL << NormalSQLParser::LT)
            | (1ULL << NormalSQLParser::LT_EQ)
            | (1ULL << NormalSQLParser::GT)
            | (1ULL << NormalSQLParser::GT_EQ))) != 0))) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(372);
          expr(19);
          break;
        }

        case 5: {
          auto newContext = _tracker.createInstance<Expr_betweenContext>(_tracker.createInstance<ExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleExpr);
          setState(373);

          if (!(precpred(_ctx, 17))) throw FailedPredicateException(this, "precpred(_ctx, 17)");
          setState(375);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == NormalSQLParser::K_NOT) {
            setState(374);
            match(NormalSQLParser::K_NOT);
          }
          setState(377);
          match(NormalSQLParser::K_BETWEEN);
          setState(378);
          expr(0);
          setState(379);
          match(NormalSQLParser::K_AND);
          setState(380);
          expr(18);
          break;
        }

        case 6: {
          auto newContext = _tracker.createInstance<Expr_eqContext>(_tracker.createInstance<ExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleExpr);
          setState(382);

          if (!(precpred(_ctx, 16))) throw FailedPredicateException(this, "precpred(_ctx, 16)");
          setState(383);
          _la = _input->LA(1);
          if (!((((_la & ~ 0x3fULL) == 0) &&
            ((1ULL << _la) & ((1ULL << NormalSQLParser::ASSIGN)
            | (1ULL << NormalSQLParser::EQ)
            | (1ULL << NormalSQLParser::NOT_EQ1)
            | (1ULL << NormalSQLParser::NOT_EQ2))) != 0))) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(384);
          expr(17);
          break;
        }

        case 7: {
          auto newContext = _tracker.createInstance<Expr_isContext>(_tracker.createInstance<ExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleExpr);
          setState(385);

          if (!(precpred(_ctx, 9))) throw FailedPredicateException(this, "precpred(_ctx, 9)");
          setState(386);
          match(NormalSQLParser::K_IS);
          setState(388);
          _errHandler->sync(this);

          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 43, _ctx)) {
          case 1: {
            setState(387);
            match(NormalSQLParser::K_NOT);
            break;
          }

          }
          setState(390);
          expr(10);
          break;
        }

        case 8: {
          auto newContext = _tracker.createInstance<Expr_andContext>(_tracker.createInstance<ExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleExpr);
          setState(391);

          if (!(precpred(_ctx, 4))) throw FailedPredicateException(this, "precpred(_ctx, 4)");
          setState(392);
          match(NormalSQLParser::K_AND);
          setState(393);
          expr(5);
          break;
        }

        case 9: {
          auto newContext = _tracker.createInstance<Expr_andContext>(_tracker.createInstance<ExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleExpr);
          setState(394);

          if (!(precpred(_ctx, 3))) throw FailedPredicateException(this, "precpred(_ctx, 3)");
          setState(395);
          match(NormalSQLParser::T__0);
          setState(396);
          expr(4);
          break;
        }

        case 10: {
          auto newContext = _tracker.createInstance<Expr_orContext>(_tracker.createInstance<ExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleExpr);
          setState(397);

          if (!(precpred(_ctx, 2))) throw FailedPredicateException(this, "precpred(_ctx, 2)");
          setState(398);
          match(NormalSQLParser::K_OR);
          setState(399);
          expr(3);
          break;
        }

        case 11: {
          auto newContext = _tracker.createInstance<Expr_orContext>(_tracker.createInstance<ExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleExpr);
          setState(400);

          if (!(precpred(_ctx, 1))) throw FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(401);
          match(NormalSQLParser::PIPE2);
          setState(402);
          expr(2);
          break;
        }

        case 12: {
          auto newContext = _tracker.createInstance<Expr_collateContext>(_tracker.createInstance<ExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleExpr);
          setState(403);

          if (!(precpred(_ctx, 12))) throw FailedPredicateException(this, "precpred(_ctx, 12)");
          setState(404);
          match(NormalSQLParser::K_COLLATE);
          setState(405);
          collation_name();
          break;
        }

        case 13: {
          auto newContext = _tracker.createInstance<Expr_str_matchContext>(_tracker.createInstance<ExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleExpr);
          setState(406);

          if (!(precpred(_ctx, 11))) throw FailedPredicateException(this, "precpred(_ctx, 11)");
          setState(408);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == NormalSQLParser::K_NOT) {
            setState(407);
            match(NormalSQLParser::K_NOT);
          }
          setState(410);
          _la = _input->LA(1);
          if (!(((((_la - 78) & ~ 0x3fULL) == 0) &&
            ((1ULL << (_la - 78)) & ((1ULL << (NormalSQLParser::K_GLOB - 78))
            | (1ULL << (NormalSQLParser::K_LIKE - 78))
            | (1ULL << (NormalSQLParser::K_MATCH - 78))
            | (1ULL << (NormalSQLParser::K_REGEXP - 78)))) != 0))) {
          _errHandler->recoverInline(this);
          }
          else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(411);
          expr(0);
          setState(414);
          _errHandler->sync(this);

          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 45, _ctx)) {
          case 1: {
            setState(412);
            match(NormalSQLParser::K_ESCAPE);
            setState(413);
            expr(0);
            break;
          }

          }
          break;
        }

        case 14: {
          auto newContext = _tracker.createInstance<Expr_nullContext>(_tracker.createInstance<ExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleExpr);
          setState(416);

          if (!(precpred(_ctx, 10))) throw FailedPredicateException(this, "precpred(_ctx, 10)");
          setState(421);
          _errHandler->sync(this);
          switch (_input->LA(1)) {
            case NormalSQLParser::K_ISNULL: {
              setState(417);
              match(NormalSQLParser::K_ISNULL);
              break;
            }

            case NormalSQLParser::K_NOTNULL: {
              setState(418);
              match(NormalSQLParser::K_NOTNULL);
              break;
            }

            case NormalSQLParser::K_NOT: {
              setState(419);
              match(NormalSQLParser::K_NOT);
              setState(420);
              match(NormalSQLParser::K_NULL);
              break;
            }

          default:
            throw NoViableAltException(this);
          }
          break;
        }

        case 15: {
          auto newContext = _tracker.createInstance<Expr_inContext>(_tracker.createInstance<ExprContext>(parentContext, parentState));
          _localctx = newContext;
          pushNewRecursionContext(newContext, startState, RuleExpr);
          setState(423);

          if (!(precpred(_ctx, 8))) throw FailedPredicateException(this, "precpred(_ctx, 8)");
          setState(425);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == NormalSQLParser::K_NOT) {
            setState(424);
            match(NormalSQLParser::K_NOT);
          }
          setState(427);
          match(NormalSQLParser::K_IN);
          setState(447);
          _errHandler->sync(this);
          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 51, _ctx)) {
          case 1: {
            setState(428);
            match(NormalSQLParser::OPEN_PAR);
            setState(438);
            _errHandler->sync(this);

            switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 49, _ctx)) {
            case 1: {
              setState(429);
              select_stmt();
              break;
            }

            case 2: {
              setState(430);
              expr(0);
              setState(435);
              _errHandler->sync(this);
              _la = _input->LA(1);
              while (_la == NormalSQLParser::COMMA) {
                setState(431);
                match(NormalSQLParser::COMMA);
                setState(432);
                expr(0);
                setState(437);
                _errHandler->sync(this);
                _la = _input->LA(1);
              }
              break;
            }

            }
            setState(440);
            match(NormalSQLParser::CLOSE_PAR);
            break;
          }

          case 2: {
            setState(444);
            _errHandler->sync(this);

            switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 50, _ctx)) {
            case 1: {
              setState(441);
              database_name();
              setState(442);
              match(NormalSQLParser::DOT);
              break;
            }

            }
            setState(446);
            table_name();
            break;
          }

          }
          break;
        }

        } 
      }
      setState(453);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 53, _ctx);
    }
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- Raise_functionContext ------------------------------------------------------------------

NormalSQLParser::Raise_functionContext::Raise_functionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::Raise_functionContext::K_RAISE() {
  return getToken(NormalSQLParser::K_RAISE, 0);
}

tree::TerminalNode* NormalSQLParser::Raise_functionContext::OPEN_PAR() {
  return getToken(NormalSQLParser::OPEN_PAR, 0);
}

tree::TerminalNode* NormalSQLParser::Raise_functionContext::CLOSE_PAR() {
  return getToken(NormalSQLParser::CLOSE_PAR, 0);
}

tree::TerminalNode* NormalSQLParser::Raise_functionContext::K_IGNORE() {
  return getToken(NormalSQLParser::K_IGNORE, 0);
}

tree::TerminalNode* NormalSQLParser::Raise_functionContext::COMMA() {
  return getToken(NormalSQLParser::COMMA, 0);
}

NormalSQLParser::Error_messageContext* NormalSQLParser::Raise_functionContext::error_message() {
  return getRuleContext<NormalSQLParser::Error_messageContext>(0);
}

tree::TerminalNode* NormalSQLParser::Raise_functionContext::K_ROLLBACK() {
  return getToken(NormalSQLParser::K_ROLLBACK, 0);
}

tree::TerminalNode* NormalSQLParser::Raise_functionContext::K_ABORT() {
  return getToken(NormalSQLParser::K_ABORT, 0);
}

tree::TerminalNode* NormalSQLParser::Raise_functionContext::K_FAIL() {
  return getToken(NormalSQLParser::K_FAIL, 0);
}


size_t NormalSQLParser::Raise_functionContext::getRuleIndex() const {
  return NormalSQLParser::RuleRaise_function;
}

void NormalSQLParser::Raise_functionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterRaise_function(this);
}

void NormalSQLParser::Raise_functionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitRaise_function(this);
}


antlrcpp::Any NormalSQLParser::Raise_functionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitRaise_function(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Raise_functionContext* NormalSQLParser::raise_function() {
  Raise_functionContext *_localctx = _tracker.createInstance<Raise_functionContext>(_ctx, getState());
  enterRule(_localctx, 22, NormalSQLParser::RuleRaise_function);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(454);
    match(NormalSQLParser::K_RAISE);
    setState(455);
    match(NormalSQLParser::OPEN_PAR);
    setState(460);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case NormalSQLParser::K_IGNORE: {
        setState(456);
        match(NormalSQLParser::K_IGNORE);
        break;
      }

      case NormalSQLParser::K_ABORT:
      case NormalSQLParser::K_FAIL:
      case NormalSQLParser::K_ROLLBACK: {
        setState(457);
        _la = _input->LA(1);
        if (!(_la == NormalSQLParser::K_ABORT || _la == NormalSQLParser::K_FAIL

        || _la == NormalSQLParser::K_ROLLBACK)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(458);
        match(NormalSQLParser::COMMA);
        setState(459);
        error_message();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(462);
    match(NormalSQLParser::CLOSE_PAR);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Indexed_columnContext ------------------------------------------------------------------

NormalSQLParser::Indexed_columnContext::Indexed_columnContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Column_nameContext* NormalSQLParser::Indexed_columnContext::column_name() {
  return getRuleContext<NormalSQLParser::Column_nameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Indexed_columnContext::K_COLLATE() {
  return getToken(NormalSQLParser::K_COLLATE, 0);
}

NormalSQLParser::Collation_nameContext* NormalSQLParser::Indexed_columnContext::collation_name() {
  return getRuleContext<NormalSQLParser::Collation_nameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Indexed_columnContext::K_ASC() {
  return getToken(NormalSQLParser::K_ASC, 0);
}

tree::TerminalNode* NormalSQLParser::Indexed_columnContext::K_DESC() {
  return getToken(NormalSQLParser::K_DESC, 0);
}


size_t NormalSQLParser::Indexed_columnContext::getRuleIndex() const {
  return NormalSQLParser::RuleIndexed_column;
}

void NormalSQLParser::Indexed_columnContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterIndexed_column(this);
}

void NormalSQLParser::Indexed_columnContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitIndexed_column(this);
}


antlrcpp::Any NormalSQLParser::Indexed_columnContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitIndexed_column(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Indexed_columnContext* NormalSQLParser::indexed_column() {
  Indexed_columnContext *_localctx = _tracker.createInstance<Indexed_columnContext>(_ctx, getState());
  enterRule(_localctx, 24, NormalSQLParser::RuleIndexed_column);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(464);
    column_name();
    setState(467);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NormalSQLParser::K_COLLATE) {
      setState(465);
      match(NormalSQLParser::K_COLLATE);
      setState(466);
      collation_name();
    }
    setState(470);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NormalSQLParser::K_ASC

    || _la == NormalSQLParser::K_DESC) {
      setState(469);
      _la = _input->LA(1);
      if (!(_la == NormalSQLParser::K_ASC

      || _la == NormalSQLParser::K_DESC)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- With_clauseContext ------------------------------------------------------------------

NormalSQLParser::With_clauseContext::With_clauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::With_clauseContext::K_WITH() {
  return getToken(NormalSQLParser::K_WITH, 0);
}

std::vector<NormalSQLParser::Cte_table_nameContext *> NormalSQLParser::With_clauseContext::cte_table_name() {
  return getRuleContexts<NormalSQLParser::Cte_table_nameContext>();
}

NormalSQLParser::Cte_table_nameContext* NormalSQLParser::With_clauseContext::cte_table_name(size_t i) {
  return getRuleContext<NormalSQLParser::Cte_table_nameContext>(i);
}

std::vector<tree::TerminalNode *> NormalSQLParser::With_clauseContext::K_AS() {
  return getTokens(NormalSQLParser::K_AS);
}

tree::TerminalNode* NormalSQLParser::With_clauseContext::K_AS(size_t i) {
  return getToken(NormalSQLParser::K_AS, i);
}

std::vector<tree::TerminalNode *> NormalSQLParser::With_clauseContext::OPEN_PAR() {
  return getTokens(NormalSQLParser::OPEN_PAR);
}

tree::TerminalNode* NormalSQLParser::With_clauseContext::OPEN_PAR(size_t i) {
  return getToken(NormalSQLParser::OPEN_PAR, i);
}

std::vector<NormalSQLParser::Select_stmtContext *> NormalSQLParser::With_clauseContext::select_stmt() {
  return getRuleContexts<NormalSQLParser::Select_stmtContext>();
}

NormalSQLParser::Select_stmtContext* NormalSQLParser::With_clauseContext::select_stmt(size_t i) {
  return getRuleContext<NormalSQLParser::Select_stmtContext>(i);
}

std::vector<tree::TerminalNode *> NormalSQLParser::With_clauseContext::CLOSE_PAR() {
  return getTokens(NormalSQLParser::CLOSE_PAR);
}

tree::TerminalNode* NormalSQLParser::With_clauseContext::CLOSE_PAR(size_t i) {
  return getToken(NormalSQLParser::CLOSE_PAR, i);
}

tree::TerminalNode* NormalSQLParser::With_clauseContext::K_RECURSIVE() {
  return getToken(NormalSQLParser::K_RECURSIVE, 0);
}

std::vector<tree::TerminalNode *> NormalSQLParser::With_clauseContext::COMMA() {
  return getTokens(NormalSQLParser::COMMA);
}

tree::TerminalNode* NormalSQLParser::With_clauseContext::COMMA(size_t i) {
  return getToken(NormalSQLParser::COMMA, i);
}


size_t NormalSQLParser::With_clauseContext::getRuleIndex() const {
  return NormalSQLParser::RuleWith_clause;
}

void NormalSQLParser::With_clauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterWith_clause(this);
}

void NormalSQLParser::With_clauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitWith_clause(this);
}


antlrcpp::Any NormalSQLParser::With_clauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitWith_clause(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::With_clauseContext* NormalSQLParser::with_clause() {
  With_clauseContext *_localctx = _tracker.createInstance<With_clauseContext>(_ctx, getState());
  enterRule(_localctx, 26, NormalSQLParser::RuleWith_clause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(472);
    match(NormalSQLParser::K_WITH);
    setState(474);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 57, _ctx)) {
    case 1: {
      setState(473);
      match(NormalSQLParser::K_RECURSIVE);
      break;
    }

    }
    setState(476);
    cte_table_name();
    setState(477);
    match(NormalSQLParser::K_AS);
    setState(478);
    match(NormalSQLParser::OPEN_PAR);
    setState(479);
    select_stmt();
    setState(480);
    match(NormalSQLParser::CLOSE_PAR);
    setState(490);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == NormalSQLParser::COMMA) {
      setState(481);
      match(NormalSQLParser::COMMA);
      setState(482);
      cte_table_name();
      setState(483);
      match(NormalSQLParser::K_AS);
      setState(484);
      match(NormalSQLParser::OPEN_PAR);
      setState(485);
      select_stmt();
      setState(486);
      match(NormalSQLParser::CLOSE_PAR);
      setState(492);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Qualified_table_nameContext ------------------------------------------------------------------

NormalSQLParser::Qualified_table_nameContext::Qualified_table_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Table_nameContext* NormalSQLParser::Qualified_table_nameContext::table_name() {
  return getRuleContext<NormalSQLParser::Table_nameContext>(0);
}

NormalSQLParser::Database_nameContext* NormalSQLParser::Qualified_table_nameContext::database_name() {
  return getRuleContext<NormalSQLParser::Database_nameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Qualified_table_nameContext::DOT() {
  return getToken(NormalSQLParser::DOT, 0);
}

tree::TerminalNode* NormalSQLParser::Qualified_table_nameContext::K_INDEXED() {
  return getToken(NormalSQLParser::K_INDEXED, 0);
}

tree::TerminalNode* NormalSQLParser::Qualified_table_nameContext::K_BY() {
  return getToken(NormalSQLParser::K_BY, 0);
}

NormalSQLParser::Index_nameContext* NormalSQLParser::Qualified_table_nameContext::index_name() {
  return getRuleContext<NormalSQLParser::Index_nameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Qualified_table_nameContext::K_NOT() {
  return getToken(NormalSQLParser::K_NOT, 0);
}


size_t NormalSQLParser::Qualified_table_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleQualified_table_name;
}

void NormalSQLParser::Qualified_table_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterQualified_table_name(this);
}

void NormalSQLParser::Qualified_table_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitQualified_table_name(this);
}


antlrcpp::Any NormalSQLParser::Qualified_table_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitQualified_table_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Qualified_table_nameContext* NormalSQLParser::qualified_table_name() {
  Qualified_table_nameContext *_localctx = _tracker.createInstance<Qualified_table_nameContext>(_ctx, getState());
  enterRule(_localctx, 28, NormalSQLParser::RuleQualified_table_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(496);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 59, _ctx)) {
    case 1: {
      setState(493);
      database_name();
      setState(494);
      match(NormalSQLParser::DOT);
      break;
    }

    }
    setState(498);
    table_name();
    setState(504);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case NormalSQLParser::K_INDEXED: {
        setState(499);
        match(NormalSQLParser::K_INDEXED);
        setState(500);
        match(NormalSQLParser::K_BY);
        setState(501);
        index_name();
        break;
      }

      case NormalSQLParser::K_NOT: {
        setState(502);
        match(NormalSQLParser::K_NOT);
        setState(503);
        match(NormalSQLParser::K_INDEXED);
        break;
      }

      case NormalSQLParser::EOF: {
        break;
      }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Ordering_termContext ------------------------------------------------------------------

NormalSQLParser::Ordering_termContext::Ordering_termContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::ExprContext* NormalSQLParser::Ordering_termContext::expr() {
  return getRuleContext<NormalSQLParser::ExprContext>(0);
}

tree::TerminalNode* NormalSQLParser::Ordering_termContext::K_COLLATE() {
  return getToken(NormalSQLParser::K_COLLATE, 0);
}

NormalSQLParser::Collation_nameContext* NormalSQLParser::Ordering_termContext::collation_name() {
  return getRuleContext<NormalSQLParser::Collation_nameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Ordering_termContext::K_ASC() {
  return getToken(NormalSQLParser::K_ASC, 0);
}

tree::TerminalNode* NormalSQLParser::Ordering_termContext::K_DESC() {
  return getToken(NormalSQLParser::K_DESC, 0);
}


size_t NormalSQLParser::Ordering_termContext::getRuleIndex() const {
  return NormalSQLParser::RuleOrdering_term;
}

void NormalSQLParser::Ordering_termContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterOrdering_term(this);
}

void NormalSQLParser::Ordering_termContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitOrdering_term(this);
}


antlrcpp::Any NormalSQLParser::Ordering_termContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitOrdering_term(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Ordering_termContext* NormalSQLParser::ordering_term() {
  Ordering_termContext *_localctx = _tracker.createInstance<Ordering_termContext>(_ctx, getState());
  enterRule(_localctx, 30, NormalSQLParser::RuleOrdering_term);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(506);
    expr(0);
    setState(509);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NormalSQLParser::K_COLLATE) {
      setState(507);
      match(NormalSQLParser::K_COLLATE);
      setState(508);
      collation_name();
    }
    setState(512);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NormalSQLParser::K_ASC

    || _la == NormalSQLParser::K_DESC) {
      setState(511);
      _la = _input->LA(1);
      if (!(_la == NormalSQLParser::K_ASC

      || _la == NormalSQLParser::K_DESC)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Pragma_valueContext ------------------------------------------------------------------

NormalSQLParser::Pragma_valueContext::Pragma_valueContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Signed_numberContext* NormalSQLParser::Pragma_valueContext::signed_number() {
  return getRuleContext<NormalSQLParser::Signed_numberContext>(0);
}

NormalSQLParser::NameContext* NormalSQLParser::Pragma_valueContext::name() {
  return getRuleContext<NormalSQLParser::NameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Pragma_valueContext::STRING_LITERAL() {
  return getToken(NormalSQLParser::STRING_LITERAL, 0);
}


size_t NormalSQLParser::Pragma_valueContext::getRuleIndex() const {
  return NormalSQLParser::RulePragma_value;
}

void NormalSQLParser::Pragma_valueContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPragma_value(this);
}

void NormalSQLParser::Pragma_valueContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPragma_value(this);
}


antlrcpp::Any NormalSQLParser::Pragma_valueContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitPragma_value(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Pragma_valueContext* NormalSQLParser::pragma_value() {
  Pragma_valueContext *_localctx = _tracker.createInstance<Pragma_valueContext>(_ctx, getState());
  enterRule(_localctx, 32, NormalSQLParser::RulePragma_value);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(517);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 63, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(514);
      signed_number();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(515);
      name();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(516);
      match(NormalSQLParser::STRING_LITERAL);
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Common_table_expressionContext ------------------------------------------------------------------

NormalSQLParser::Common_table_expressionContext::Common_table_expressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Table_nameContext* NormalSQLParser::Common_table_expressionContext::table_name() {
  return getRuleContext<NormalSQLParser::Table_nameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Common_table_expressionContext::K_AS() {
  return getToken(NormalSQLParser::K_AS, 0);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Common_table_expressionContext::OPEN_PAR() {
  return getTokens(NormalSQLParser::OPEN_PAR);
}

tree::TerminalNode* NormalSQLParser::Common_table_expressionContext::OPEN_PAR(size_t i) {
  return getToken(NormalSQLParser::OPEN_PAR, i);
}

NormalSQLParser::Select_stmtContext* NormalSQLParser::Common_table_expressionContext::select_stmt() {
  return getRuleContext<NormalSQLParser::Select_stmtContext>(0);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Common_table_expressionContext::CLOSE_PAR() {
  return getTokens(NormalSQLParser::CLOSE_PAR);
}

tree::TerminalNode* NormalSQLParser::Common_table_expressionContext::CLOSE_PAR(size_t i) {
  return getToken(NormalSQLParser::CLOSE_PAR, i);
}

std::vector<NormalSQLParser::Column_nameContext *> NormalSQLParser::Common_table_expressionContext::column_name() {
  return getRuleContexts<NormalSQLParser::Column_nameContext>();
}

NormalSQLParser::Column_nameContext* NormalSQLParser::Common_table_expressionContext::column_name(size_t i) {
  return getRuleContext<NormalSQLParser::Column_nameContext>(i);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Common_table_expressionContext::COMMA() {
  return getTokens(NormalSQLParser::COMMA);
}

tree::TerminalNode* NormalSQLParser::Common_table_expressionContext::COMMA(size_t i) {
  return getToken(NormalSQLParser::COMMA, i);
}


size_t NormalSQLParser::Common_table_expressionContext::getRuleIndex() const {
  return NormalSQLParser::RuleCommon_table_expression;
}

void NormalSQLParser::Common_table_expressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterCommon_table_expression(this);
}

void NormalSQLParser::Common_table_expressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitCommon_table_expression(this);
}


antlrcpp::Any NormalSQLParser::Common_table_expressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitCommon_table_expression(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Common_table_expressionContext* NormalSQLParser::common_table_expression() {
  Common_table_expressionContext *_localctx = _tracker.createInstance<Common_table_expressionContext>(_ctx, getState());
  enterRule(_localctx, 34, NormalSQLParser::RuleCommon_table_expression);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(519);
    table_name();
    setState(531);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NormalSQLParser::OPEN_PAR) {
      setState(520);
      match(NormalSQLParser::OPEN_PAR);
      setState(521);
      column_name();
      setState(526);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == NormalSQLParser::COMMA) {
        setState(522);
        match(NormalSQLParser::COMMA);
        setState(523);
        column_name();
        setState(528);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(529);
      match(NormalSQLParser::CLOSE_PAR);
    }
    setState(533);
    match(NormalSQLParser::K_AS);
    setState(534);
    match(NormalSQLParser::OPEN_PAR);
    setState(535);
    select_stmt();
    setState(536);
    match(NormalSQLParser::CLOSE_PAR);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Result_columnContext ------------------------------------------------------------------

NormalSQLParser::Result_columnContext::Result_columnContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::Result_columnContext::STAR() {
  return getToken(NormalSQLParser::STAR, 0);
}

NormalSQLParser::Table_nameContext* NormalSQLParser::Result_columnContext::table_name() {
  return getRuleContext<NormalSQLParser::Table_nameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Result_columnContext::DOT() {
  return getToken(NormalSQLParser::DOT, 0);
}

NormalSQLParser::ExprContext* NormalSQLParser::Result_columnContext::expr() {
  return getRuleContext<NormalSQLParser::ExprContext>(0);
}

NormalSQLParser::Column_aliasContext* NormalSQLParser::Result_columnContext::column_alias() {
  return getRuleContext<NormalSQLParser::Column_aliasContext>(0);
}

tree::TerminalNode* NormalSQLParser::Result_columnContext::K_AS() {
  return getToken(NormalSQLParser::K_AS, 0);
}


size_t NormalSQLParser::Result_columnContext::getRuleIndex() const {
  return NormalSQLParser::RuleResult_column;
}

void NormalSQLParser::Result_columnContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterResult_column(this);
}

void NormalSQLParser::Result_columnContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitResult_column(this);
}


antlrcpp::Any NormalSQLParser::Result_columnContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitResult_column(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Result_columnContext* NormalSQLParser::result_column() {
  Result_columnContext *_localctx = _tracker.createInstance<Result_columnContext>(_ctx, getState());
  enterRule(_localctx, 36, NormalSQLParser::RuleResult_column);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(550);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 68, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(538);
      match(NormalSQLParser::STAR);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(539);
      table_name();
      setState(540);
      match(NormalSQLParser::DOT);
      setState(541);
      match(NormalSQLParser::STAR);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(543);
      expr(0);
      setState(548);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == NormalSQLParser::K_AS || _la == NormalSQLParser::IDENTIFIER

      || _la == NormalSQLParser::STRING_LITERAL) {
        setState(545);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NormalSQLParser::K_AS) {
          setState(544);
          match(NormalSQLParser::K_AS);
        }
        setState(547);
        column_alias();
      }
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Table_or_subqueryContext ------------------------------------------------------------------

NormalSQLParser::Table_or_subqueryContext::Table_or_subqueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Table_nameContext* NormalSQLParser::Table_or_subqueryContext::table_name() {
  return getRuleContext<NormalSQLParser::Table_nameContext>(0);
}

NormalSQLParser::Database_nameContext* NormalSQLParser::Table_or_subqueryContext::database_name() {
  return getRuleContext<NormalSQLParser::Database_nameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Table_or_subqueryContext::DOT() {
  return getToken(NormalSQLParser::DOT, 0);
}

NormalSQLParser::Table_aliasContext* NormalSQLParser::Table_or_subqueryContext::table_alias() {
  return getRuleContext<NormalSQLParser::Table_aliasContext>(0);
}

tree::TerminalNode* NormalSQLParser::Table_or_subqueryContext::K_INDEXED() {
  return getToken(NormalSQLParser::K_INDEXED, 0);
}

tree::TerminalNode* NormalSQLParser::Table_or_subqueryContext::K_BY() {
  return getToken(NormalSQLParser::K_BY, 0);
}

NormalSQLParser::Index_nameContext* NormalSQLParser::Table_or_subqueryContext::index_name() {
  return getRuleContext<NormalSQLParser::Index_nameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Table_or_subqueryContext::K_NOT() {
  return getToken(NormalSQLParser::K_NOT, 0);
}

tree::TerminalNode* NormalSQLParser::Table_or_subqueryContext::K_AS() {
  return getToken(NormalSQLParser::K_AS, 0);
}

tree::TerminalNode* NormalSQLParser::Table_or_subqueryContext::OPEN_PAR() {
  return getToken(NormalSQLParser::OPEN_PAR, 0);
}

tree::TerminalNode* NormalSQLParser::Table_or_subqueryContext::CLOSE_PAR() {
  return getToken(NormalSQLParser::CLOSE_PAR, 0);
}

std::vector<NormalSQLParser::Table_or_subqueryContext *> NormalSQLParser::Table_or_subqueryContext::table_or_subquery() {
  return getRuleContexts<NormalSQLParser::Table_or_subqueryContext>();
}

NormalSQLParser::Table_or_subqueryContext* NormalSQLParser::Table_or_subqueryContext::table_or_subquery(size_t i) {
  return getRuleContext<NormalSQLParser::Table_or_subqueryContext>(i);
}

NormalSQLParser::Join_clauseContext* NormalSQLParser::Table_or_subqueryContext::join_clause() {
  return getRuleContext<NormalSQLParser::Join_clauseContext>(0);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Table_or_subqueryContext::COMMA() {
  return getTokens(NormalSQLParser::COMMA);
}

tree::TerminalNode* NormalSQLParser::Table_or_subqueryContext::COMMA(size_t i) {
  return getToken(NormalSQLParser::COMMA, i);
}

NormalSQLParser::Select_stmtContext* NormalSQLParser::Table_or_subqueryContext::select_stmt() {
  return getRuleContext<NormalSQLParser::Select_stmtContext>(0);
}


size_t NormalSQLParser::Table_or_subqueryContext::getRuleIndex() const {
  return NormalSQLParser::RuleTable_or_subquery;
}

void NormalSQLParser::Table_or_subqueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTable_or_subquery(this);
}

void NormalSQLParser::Table_or_subqueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTable_or_subquery(this);
}


antlrcpp::Any NormalSQLParser::Table_or_subqueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitTable_or_subquery(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Table_or_subqueryContext* NormalSQLParser::table_or_subquery() {
  Table_or_subqueryContext *_localctx = _tracker.createInstance<Table_or_subqueryContext>(_ctx, getState());
  enterRule(_localctx, 38, NormalSQLParser::RuleTable_or_subquery);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(599);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 79, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(555);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 69, _ctx)) {
      case 1: {
        setState(552);
        database_name();
        setState(553);
        match(NormalSQLParser::DOT);
        break;
      }

      }
      setState(557);
      table_name();
      setState(562);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 71, _ctx)) {
      case 1: {
        setState(559);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 70, _ctx)) {
        case 1: {
          setState(558);
          match(NormalSQLParser::K_AS);
          break;
        }

        }
        setState(561);
        table_alias();
        break;
      }

      }
      setState(569);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case NormalSQLParser::K_INDEXED: {
          setState(564);
          match(NormalSQLParser::K_INDEXED);
          setState(565);
          match(NormalSQLParser::K_BY);
          setState(566);
          index_name();
          break;
        }

        case NormalSQLParser::K_NOT: {
          setState(567);
          match(NormalSQLParser::K_NOT);
          setState(568);
          match(NormalSQLParser::K_INDEXED);
          break;
        }

        case NormalSQLParser::EOF:
        case NormalSQLParser::SCOL:
        case NormalSQLParser::CLOSE_PAR:
        case NormalSQLParser::COMMA:
        case NormalSQLParser::K_CROSS:
        case NormalSQLParser::K_EXCEPT:
        case NormalSQLParser::K_EXPLAIN:
        case NormalSQLParser::K_GROUP:
        case NormalSQLParser::K_INNER:
        case NormalSQLParser::K_INTERSECT:
        case NormalSQLParser::K_JOIN:
        case NormalSQLParser::K_LEFT:
        case NormalSQLParser::K_LIMIT:
        case NormalSQLParser::K_NATURAL:
        case NormalSQLParser::K_ON:
        case NormalSQLParser::K_ORDER:
        case NormalSQLParser::K_SELECT:
        case NormalSQLParser::K_UNION:
        case NormalSQLParser::K_USING:
        case NormalSQLParser::K_VALUES:
        case NormalSQLParser::K_WHERE:
        case NormalSQLParser::K_WITH:
        case NormalSQLParser::UNEXPECTED_CHAR: {
          break;
        }

      default:
        break;
      }
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(571);
      match(NormalSQLParser::OPEN_PAR);
      setState(581);
      _errHandler->sync(this);
      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 74, _ctx)) {
      case 1: {
        setState(572);
        table_or_subquery();
        setState(577);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NormalSQLParser::COMMA) {
          setState(573);
          match(NormalSQLParser::COMMA);
          setState(574);
          table_or_subquery();
          setState(579);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        break;
      }

      case 2: {
        setState(580);
        join_clause();
        break;
      }

      }
      setState(583);
      match(NormalSQLParser::CLOSE_PAR);
      setState(588);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 76, _ctx)) {
      case 1: {
        setState(585);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 75, _ctx)) {
        case 1: {
          setState(584);
          match(NormalSQLParser::K_AS);
          break;
        }

        }
        setState(587);
        table_alias();
        break;
      }

      }
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(590);
      match(NormalSQLParser::OPEN_PAR);
      setState(591);
      select_stmt();
      setState(592);
      match(NormalSQLParser::CLOSE_PAR);
      setState(597);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 78, _ctx)) {
      case 1: {
        setState(594);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 77, _ctx)) {
        case 1: {
          setState(593);
          match(NormalSQLParser::K_AS);
          break;
        }

        }
        setState(596);
        table_alias();
        break;
      }

      }
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Join_clauseContext ------------------------------------------------------------------

NormalSQLParser::Join_clauseContext::Join_clauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<NormalSQLParser::Table_or_subqueryContext *> NormalSQLParser::Join_clauseContext::table_or_subquery() {
  return getRuleContexts<NormalSQLParser::Table_or_subqueryContext>();
}

NormalSQLParser::Table_or_subqueryContext* NormalSQLParser::Join_clauseContext::table_or_subquery(size_t i) {
  return getRuleContext<NormalSQLParser::Table_or_subqueryContext>(i);
}

std::vector<NormalSQLParser::Join_operatorContext *> NormalSQLParser::Join_clauseContext::join_operator() {
  return getRuleContexts<NormalSQLParser::Join_operatorContext>();
}

NormalSQLParser::Join_operatorContext* NormalSQLParser::Join_clauseContext::join_operator(size_t i) {
  return getRuleContext<NormalSQLParser::Join_operatorContext>(i);
}

std::vector<NormalSQLParser::Join_constraintContext *> NormalSQLParser::Join_clauseContext::join_constraint() {
  return getRuleContexts<NormalSQLParser::Join_constraintContext>();
}

NormalSQLParser::Join_constraintContext* NormalSQLParser::Join_clauseContext::join_constraint(size_t i) {
  return getRuleContext<NormalSQLParser::Join_constraintContext>(i);
}


size_t NormalSQLParser::Join_clauseContext::getRuleIndex() const {
  return NormalSQLParser::RuleJoin_clause;
}

void NormalSQLParser::Join_clauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterJoin_clause(this);
}

void NormalSQLParser::Join_clauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitJoin_clause(this);
}


antlrcpp::Any NormalSQLParser::Join_clauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitJoin_clause(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Join_clauseContext* NormalSQLParser::join_clause() {
  Join_clauseContext *_localctx = _tracker.createInstance<Join_clauseContext>(_ctx, getState());
  enterRule(_localctx, 40, NormalSQLParser::RuleJoin_clause);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(601);
    table_or_subquery();
    setState(608);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == NormalSQLParser::COMMA

    || _la == NormalSQLParser::K_CROSS || ((((_la - 88) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 88)) & ((1ULL << (NormalSQLParser::K_INNER - 88))
      | (1ULL << (NormalSQLParser::K_JOIN - 88))
      | (1ULL << (NormalSQLParser::K_LEFT - 88))
      | (1ULL << (NormalSQLParser::K_NATURAL - 88)))) != 0)) {
      setState(602);
      join_operator();
      setState(603);
      table_or_subquery();
      setState(604);
      join_constraint();
      setState(610);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Join_operatorContext ------------------------------------------------------------------

NormalSQLParser::Join_operatorContext::Join_operatorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::Join_operatorContext::COMMA() {
  return getToken(NormalSQLParser::COMMA, 0);
}

tree::TerminalNode* NormalSQLParser::Join_operatorContext::K_JOIN() {
  return getToken(NormalSQLParser::K_JOIN, 0);
}

tree::TerminalNode* NormalSQLParser::Join_operatorContext::K_NATURAL() {
  return getToken(NormalSQLParser::K_NATURAL, 0);
}

tree::TerminalNode* NormalSQLParser::Join_operatorContext::K_LEFT() {
  return getToken(NormalSQLParser::K_LEFT, 0);
}

tree::TerminalNode* NormalSQLParser::Join_operatorContext::K_INNER() {
  return getToken(NormalSQLParser::K_INNER, 0);
}

tree::TerminalNode* NormalSQLParser::Join_operatorContext::K_CROSS() {
  return getToken(NormalSQLParser::K_CROSS, 0);
}

tree::TerminalNode* NormalSQLParser::Join_operatorContext::K_OUTER() {
  return getToken(NormalSQLParser::K_OUTER, 0);
}


size_t NormalSQLParser::Join_operatorContext::getRuleIndex() const {
  return NormalSQLParser::RuleJoin_operator;
}

void NormalSQLParser::Join_operatorContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterJoin_operator(this);
}

void NormalSQLParser::Join_operatorContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitJoin_operator(this);
}


antlrcpp::Any NormalSQLParser::Join_operatorContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitJoin_operator(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Join_operatorContext* NormalSQLParser::join_operator() {
  Join_operatorContext *_localctx = _tracker.createInstance<Join_operatorContext>(_ctx, getState());
  enterRule(_localctx, 42, NormalSQLParser::RuleJoin_operator);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(624);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case NormalSQLParser::COMMA: {
        enterOuterAlt(_localctx, 1);
        setState(611);
        match(NormalSQLParser::COMMA);
        break;
      }

      case NormalSQLParser::K_CROSS:
      case NormalSQLParser::K_INNER:
      case NormalSQLParser::K_JOIN:
      case NormalSQLParser::K_LEFT:
      case NormalSQLParser::K_NATURAL: {
        enterOuterAlt(_localctx, 2);
        setState(613);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == NormalSQLParser::K_NATURAL) {
          setState(612);
          match(NormalSQLParser::K_NATURAL);
        }
        setState(621);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case NormalSQLParser::K_LEFT: {
            setState(615);
            match(NormalSQLParser::K_LEFT);
            setState(617);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == NormalSQLParser::K_OUTER) {
              setState(616);
              match(NormalSQLParser::K_OUTER);
            }
            break;
          }

          case NormalSQLParser::K_INNER: {
            setState(619);
            match(NormalSQLParser::K_INNER);
            break;
          }

          case NormalSQLParser::K_CROSS: {
            setState(620);
            match(NormalSQLParser::K_CROSS);
            break;
          }

          case NormalSQLParser::K_JOIN: {
            break;
          }

        default:
          break;
        }
        setState(623);
        match(NormalSQLParser::K_JOIN);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Join_constraintContext ------------------------------------------------------------------

NormalSQLParser::Join_constraintContext::Join_constraintContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::Join_constraintContext::K_ON() {
  return getToken(NormalSQLParser::K_ON, 0);
}

NormalSQLParser::ExprContext* NormalSQLParser::Join_constraintContext::expr() {
  return getRuleContext<NormalSQLParser::ExprContext>(0);
}

tree::TerminalNode* NormalSQLParser::Join_constraintContext::K_USING() {
  return getToken(NormalSQLParser::K_USING, 0);
}

tree::TerminalNode* NormalSQLParser::Join_constraintContext::OPEN_PAR() {
  return getToken(NormalSQLParser::OPEN_PAR, 0);
}

std::vector<NormalSQLParser::Column_nameContext *> NormalSQLParser::Join_constraintContext::column_name() {
  return getRuleContexts<NormalSQLParser::Column_nameContext>();
}

NormalSQLParser::Column_nameContext* NormalSQLParser::Join_constraintContext::column_name(size_t i) {
  return getRuleContext<NormalSQLParser::Column_nameContext>(i);
}

tree::TerminalNode* NormalSQLParser::Join_constraintContext::CLOSE_PAR() {
  return getToken(NormalSQLParser::CLOSE_PAR, 0);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Join_constraintContext::COMMA() {
  return getTokens(NormalSQLParser::COMMA);
}

tree::TerminalNode* NormalSQLParser::Join_constraintContext::COMMA(size_t i) {
  return getToken(NormalSQLParser::COMMA, i);
}


size_t NormalSQLParser::Join_constraintContext::getRuleIndex() const {
  return NormalSQLParser::RuleJoin_constraint;
}

void NormalSQLParser::Join_constraintContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterJoin_constraint(this);
}

void NormalSQLParser::Join_constraintContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitJoin_constraint(this);
}


antlrcpp::Any NormalSQLParser::Join_constraintContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitJoin_constraint(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Join_constraintContext* NormalSQLParser::join_constraint() {
  Join_constraintContext *_localctx = _tracker.createInstance<Join_constraintContext>(_ctx, getState());
  enterRule(_localctx, 44, NormalSQLParser::RuleJoin_constraint);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(640);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case NormalSQLParser::K_ON: {
        setState(626);
        match(NormalSQLParser::K_ON);
        setState(627);
        expr(0);
        break;
      }

      case NormalSQLParser::K_USING: {
        setState(628);
        match(NormalSQLParser::K_USING);
        setState(629);
        match(NormalSQLParser::OPEN_PAR);
        setState(630);
        column_name();
        setState(635);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == NormalSQLParser::COMMA) {
          setState(631);
          match(NormalSQLParser::COMMA);
          setState(632);
          column_name();
          setState(637);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(638);
        match(NormalSQLParser::CLOSE_PAR);
        break;
      }

      case NormalSQLParser::EOF:
      case NormalSQLParser::SCOL:
      case NormalSQLParser::CLOSE_PAR:
      case NormalSQLParser::COMMA:
      case NormalSQLParser::K_CROSS:
      case NormalSQLParser::K_EXCEPT:
      case NormalSQLParser::K_EXPLAIN:
      case NormalSQLParser::K_GROUP:
      case NormalSQLParser::K_INNER:
      case NormalSQLParser::K_INTERSECT:
      case NormalSQLParser::K_JOIN:
      case NormalSQLParser::K_LEFT:
      case NormalSQLParser::K_LIMIT:
      case NormalSQLParser::K_NATURAL:
      case NormalSQLParser::K_ORDER:
      case NormalSQLParser::K_SELECT:
      case NormalSQLParser::K_UNION:
      case NormalSQLParser::K_VALUES:
      case NormalSQLParser::K_WHERE:
      case NormalSQLParser::K_WITH:
      case NormalSQLParser::UNEXPECTED_CHAR: {
        break;
      }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Compound_operatorContext ------------------------------------------------------------------

NormalSQLParser::Compound_operatorContext::Compound_operatorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::Compound_operatorContext::K_UNION() {
  return getToken(NormalSQLParser::K_UNION, 0);
}

tree::TerminalNode* NormalSQLParser::Compound_operatorContext::K_ALL() {
  return getToken(NormalSQLParser::K_ALL, 0);
}

tree::TerminalNode* NormalSQLParser::Compound_operatorContext::K_INTERSECT() {
  return getToken(NormalSQLParser::K_INTERSECT, 0);
}

tree::TerminalNode* NormalSQLParser::Compound_operatorContext::K_EXCEPT() {
  return getToken(NormalSQLParser::K_EXCEPT, 0);
}


size_t NormalSQLParser::Compound_operatorContext::getRuleIndex() const {
  return NormalSQLParser::RuleCompound_operator;
}

void NormalSQLParser::Compound_operatorContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterCompound_operator(this);
}

void NormalSQLParser::Compound_operatorContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitCompound_operator(this);
}


antlrcpp::Any NormalSQLParser::Compound_operatorContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitCompound_operator(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Compound_operatorContext* NormalSQLParser::compound_operator() {
  Compound_operatorContext *_localctx = _tracker.createInstance<Compound_operatorContext>(_ctx, getState());
  enterRule(_localctx, 46, NormalSQLParser::RuleCompound_operator);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(647);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 87, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(642);
      match(NormalSQLParser::K_UNION);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(643);
      match(NormalSQLParser::K_UNION);
      setState(644);
      match(NormalSQLParser::K_ALL);
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(645);
      match(NormalSQLParser::K_INTERSECT);
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(646);
      match(NormalSQLParser::K_EXCEPT);
      break;
    }

    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Cte_table_nameContext ------------------------------------------------------------------

NormalSQLParser::Cte_table_nameContext::Cte_table_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Table_nameContext* NormalSQLParser::Cte_table_nameContext::table_name() {
  return getRuleContext<NormalSQLParser::Table_nameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Cte_table_nameContext::OPEN_PAR() {
  return getToken(NormalSQLParser::OPEN_PAR, 0);
}

std::vector<NormalSQLParser::Column_nameContext *> NormalSQLParser::Cte_table_nameContext::column_name() {
  return getRuleContexts<NormalSQLParser::Column_nameContext>();
}

NormalSQLParser::Column_nameContext* NormalSQLParser::Cte_table_nameContext::column_name(size_t i) {
  return getRuleContext<NormalSQLParser::Column_nameContext>(i);
}

tree::TerminalNode* NormalSQLParser::Cte_table_nameContext::CLOSE_PAR() {
  return getToken(NormalSQLParser::CLOSE_PAR, 0);
}

std::vector<tree::TerminalNode *> NormalSQLParser::Cte_table_nameContext::COMMA() {
  return getTokens(NormalSQLParser::COMMA);
}

tree::TerminalNode* NormalSQLParser::Cte_table_nameContext::COMMA(size_t i) {
  return getToken(NormalSQLParser::COMMA, i);
}


size_t NormalSQLParser::Cte_table_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleCte_table_name;
}

void NormalSQLParser::Cte_table_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterCte_table_name(this);
}

void NormalSQLParser::Cte_table_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitCte_table_name(this);
}


antlrcpp::Any NormalSQLParser::Cte_table_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitCte_table_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Cte_table_nameContext* NormalSQLParser::cte_table_name() {
  Cte_table_nameContext *_localctx = _tracker.createInstance<Cte_table_nameContext>(_ctx, getState());
  enterRule(_localctx, 48, NormalSQLParser::RuleCte_table_name);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(649);
    table_name();
    setState(661);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NormalSQLParser::OPEN_PAR) {
      setState(650);
      match(NormalSQLParser::OPEN_PAR);
      setState(651);
      column_name();
      setState(656);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == NormalSQLParser::COMMA) {
        setState(652);
        match(NormalSQLParser::COMMA);
        setState(653);
        column_name();
        setState(658);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(659);
      match(NormalSQLParser::CLOSE_PAR);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Signed_numberContext ------------------------------------------------------------------

NormalSQLParser::Signed_numberContext::Signed_numberContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::Signed_numberContext::NUMERIC_LITERAL() {
  return getToken(NormalSQLParser::NUMERIC_LITERAL, 0);
}

tree::TerminalNode* NormalSQLParser::Signed_numberContext::PLUS() {
  return getToken(NormalSQLParser::PLUS, 0);
}

tree::TerminalNode* NormalSQLParser::Signed_numberContext::MINUS() {
  return getToken(NormalSQLParser::MINUS, 0);
}


size_t NormalSQLParser::Signed_numberContext::getRuleIndex() const {
  return NormalSQLParser::RuleSigned_number;
}

void NormalSQLParser::Signed_numberContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSigned_number(this);
}

void NormalSQLParser::Signed_numberContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSigned_number(this);
}


antlrcpp::Any NormalSQLParser::Signed_numberContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitSigned_number(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Signed_numberContext* NormalSQLParser::signed_number() {
  Signed_numberContext *_localctx = _tracker.createInstance<Signed_numberContext>(_ctx, getState());
  enterRule(_localctx, 50, NormalSQLParser::RuleSigned_number);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(664);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == NormalSQLParser::PLUS

    || _la == NormalSQLParser::MINUS) {
      setState(663);
      _la = _input->LA(1);
      if (!(_la == NormalSQLParser::PLUS

      || _la == NormalSQLParser::MINUS)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
    setState(666);
    match(NormalSQLParser::NUMERIC_LITERAL);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Literal_valueContext ------------------------------------------------------------------

NormalSQLParser::Literal_valueContext::Literal_valueContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t NormalSQLParser::Literal_valueContext::getRuleIndex() const {
  return NormalSQLParser::RuleLiteral_value;
}

void NormalSQLParser::Literal_valueContext::copyFrom(Literal_valueContext *ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- Literal_value_k_nullContext ------------------------------------------------------------------

tree::TerminalNode* NormalSQLParser::Literal_value_k_nullContext::K_NULL() {
  return getToken(NormalSQLParser::K_NULL, 0);
}

NormalSQLParser::Literal_value_k_nullContext::Literal_value_k_nullContext(Literal_valueContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Literal_value_k_nullContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLiteral_value_k_null(this);
}
void NormalSQLParser::Literal_value_k_nullContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLiteral_value_k_null(this);
}

antlrcpp::Any NormalSQLParser::Literal_value_k_nullContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitLiteral_value_k_null(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Literal_value_k_current_timeContext ------------------------------------------------------------------

tree::TerminalNode* NormalSQLParser::Literal_value_k_current_timeContext::K_CURRENT_TIME() {
  return getToken(NormalSQLParser::K_CURRENT_TIME, 0);
}

NormalSQLParser::Literal_value_k_current_timeContext::Literal_value_k_current_timeContext(Literal_valueContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Literal_value_k_current_timeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLiteral_value_k_current_time(this);
}
void NormalSQLParser::Literal_value_k_current_timeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLiteral_value_k_current_time(this);
}

antlrcpp::Any NormalSQLParser::Literal_value_k_current_timeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitLiteral_value_k_current_time(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Literal_value_stringContext ------------------------------------------------------------------

tree::TerminalNode* NormalSQLParser::Literal_value_stringContext::STRING_LITERAL() {
  return getToken(NormalSQLParser::STRING_LITERAL, 0);
}

NormalSQLParser::Literal_value_stringContext::Literal_value_stringContext(Literal_valueContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Literal_value_stringContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLiteral_value_string(this);
}
void NormalSQLParser::Literal_value_stringContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLiteral_value_string(this);
}

antlrcpp::Any NormalSQLParser::Literal_value_stringContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitLiteral_value_string(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Literal_value_blobContext ------------------------------------------------------------------

tree::TerminalNode* NormalSQLParser::Literal_value_blobContext::BLOB_LITERAL() {
  return getToken(NormalSQLParser::BLOB_LITERAL, 0);
}

NormalSQLParser::Literal_value_blobContext::Literal_value_blobContext(Literal_valueContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Literal_value_blobContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLiteral_value_blob(this);
}
void NormalSQLParser::Literal_value_blobContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLiteral_value_blob(this);
}

antlrcpp::Any NormalSQLParser::Literal_value_blobContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitLiteral_value_blob(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Literal_value_numericContext ------------------------------------------------------------------

tree::TerminalNode* NormalSQLParser::Literal_value_numericContext::NUMERIC_LITERAL() {
  return getToken(NormalSQLParser::NUMERIC_LITERAL, 0);
}

NormalSQLParser::Literal_value_numericContext::Literal_value_numericContext(Literal_valueContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Literal_value_numericContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLiteral_value_numeric(this);
}
void NormalSQLParser::Literal_value_numericContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLiteral_value_numeric(this);
}

antlrcpp::Any NormalSQLParser::Literal_value_numericContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitLiteral_value_numeric(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Literal_value_k_current_timestampContext ------------------------------------------------------------------

tree::TerminalNode* NormalSQLParser::Literal_value_k_current_timestampContext::K_CURRENT_TIMESTAMP() {
  return getToken(NormalSQLParser::K_CURRENT_TIMESTAMP, 0);
}

NormalSQLParser::Literal_value_k_current_timestampContext::Literal_value_k_current_timestampContext(Literal_valueContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Literal_value_k_current_timestampContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLiteral_value_k_current_timestamp(this);
}
void NormalSQLParser::Literal_value_k_current_timestampContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLiteral_value_k_current_timestamp(this);
}

antlrcpp::Any NormalSQLParser::Literal_value_k_current_timestampContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitLiteral_value_k_current_timestamp(this);
  else
    return visitor->visitChildren(this);
}
//----------------- Literal_value_k_current_dateContext ------------------------------------------------------------------

tree::TerminalNode* NormalSQLParser::Literal_value_k_current_dateContext::K_CURRENT_DATE() {
  return getToken(NormalSQLParser::K_CURRENT_DATE, 0);
}

NormalSQLParser::Literal_value_k_current_dateContext::Literal_value_k_current_dateContext(Literal_valueContext *ctx) { copyFrom(ctx); }

void NormalSQLParser::Literal_value_k_current_dateContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLiteral_value_k_current_date(this);
}
void NormalSQLParser::Literal_value_k_current_dateContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLiteral_value_k_current_date(this);
}

antlrcpp::Any NormalSQLParser::Literal_value_k_current_dateContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitLiteral_value_k_current_date(this);
  else
    return visitor->visitChildren(this);
}
NormalSQLParser::Literal_valueContext* NormalSQLParser::literal_value() {
  Literal_valueContext *_localctx = _tracker.createInstance<Literal_valueContext>(_ctx, getState());
  enterRule(_localctx, 52, NormalSQLParser::RuleLiteral_value);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(675);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case NormalSQLParser::NUMERIC_LITERAL: {
        _localctx = dynamic_cast<Literal_valueContext *>(_tracker.createInstance<NormalSQLParser::Literal_value_numericContext>(_localctx));
        enterOuterAlt(_localctx, 1);
        setState(668);
        match(NormalSQLParser::NUMERIC_LITERAL);
        break;
      }

      case NormalSQLParser::STRING_LITERAL: {
        _localctx = dynamic_cast<Literal_valueContext *>(_tracker.createInstance<NormalSQLParser::Literal_value_stringContext>(_localctx));
        enterOuterAlt(_localctx, 2);
        setState(669);
        match(NormalSQLParser::STRING_LITERAL);
        break;
      }

      case NormalSQLParser::BLOB_LITERAL: {
        _localctx = dynamic_cast<Literal_valueContext *>(_tracker.createInstance<NormalSQLParser::Literal_value_blobContext>(_localctx));
        enterOuterAlt(_localctx, 3);
        setState(670);
        match(NormalSQLParser::BLOB_LITERAL);
        break;
      }

      case NormalSQLParser::K_NULL: {
        _localctx = dynamic_cast<Literal_valueContext *>(_tracker.createInstance<NormalSQLParser::Literal_value_k_nullContext>(_localctx));
        enterOuterAlt(_localctx, 4);
        setState(671);
        match(NormalSQLParser::K_NULL);
        break;
      }

      case NormalSQLParser::K_CURRENT_TIME: {
        _localctx = dynamic_cast<Literal_valueContext *>(_tracker.createInstance<NormalSQLParser::Literal_value_k_current_timeContext>(_localctx));
        enterOuterAlt(_localctx, 5);
        setState(672);
        match(NormalSQLParser::K_CURRENT_TIME);
        break;
      }

      case NormalSQLParser::K_CURRENT_DATE: {
        _localctx = dynamic_cast<Literal_valueContext *>(_tracker.createInstance<NormalSQLParser::Literal_value_k_current_dateContext>(_localctx));
        enterOuterAlt(_localctx, 6);
        setState(673);
        match(NormalSQLParser::K_CURRENT_DATE);
        break;
      }

      case NormalSQLParser::K_CURRENT_TIMESTAMP: {
        _localctx = dynamic_cast<Literal_valueContext *>(_tracker.createInstance<NormalSQLParser::Literal_value_k_current_timestampContext>(_localctx));
        enterOuterAlt(_localctx, 7);
        setState(674);
        match(NormalSQLParser::K_CURRENT_TIMESTAMP);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Unary_operatorContext ------------------------------------------------------------------

NormalSQLParser::Unary_operatorContext::Unary_operatorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::Unary_operatorContext::MINUS() {
  return getToken(NormalSQLParser::MINUS, 0);
}

tree::TerminalNode* NormalSQLParser::Unary_operatorContext::PLUS() {
  return getToken(NormalSQLParser::PLUS, 0);
}

tree::TerminalNode* NormalSQLParser::Unary_operatorContext::TILDE() {
  return getToken(NormalSQLParser::TILDE, 0);
}

tree::TerminalNode* NormalSQLParser::Unary_operatorContext::K_NOT() {
  return getToken(NormalSQLParser::K_NOT, 0);
}


size_t NormalSQLParser::Unary_operatorContext::getRuleIndex() const {
  return NormalSQLParser::RuleUnary_operator;
}

void NormalSQLParser::Unary_operatorContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterUnary_operator(this);
}

void NormalSQLParser::Unary_operatorContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitUnary_operator(this);
}


antlrcpp::Any NormalSQLParser::Unary_operatorContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitUnary_operator(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Unary_operatorContext* NormalSQLParser::unary_operator() {
  Unary_operatorContext *_localctx = _tracker.createInstance<Unary_operatorContext>(_ctx, getState());
  enterRule(_localctx, 54, NormalSQLParser::RuleUnary_operator);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(677);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << NormalSQLParser::PLUS)
      | (1ULL << NormalSQLParser::MINUS)
      | (1ULL << NormalSQLParser::TILDE))) != 0) || _la == NormalSQLParser::K_NOT)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Error_messageContext ------------------------------------------------------------------

NormalSQLParser::Error_messageContext::Error_messageContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::Error_messageContext::STRING_LITERAL() {
  return getToken(NormalSQLParser::STRING_LITERAL, 0);
}


size_t NormalSQLParser::Error_messageContext::getRuleIndex() const {
  return NormalSQLParser::RuleError_message;
}

void NormalSQLParser::Error_messageContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterError_message(this);
}

void NormalSQLParser::Error_messageContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitError_message(this);
}


antlrcpp::Any NormalSQLParser::Error_messageContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitError_message(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Error_messageContext* NormalSQLParser::error_message() {
  Error_messageContext *_localctx = _tracker.createInstance<Error_messageContext>(_ctx, getState());
  enterRule(_localctx, 56, NormalSQLParser::RuleError_message);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(679);
    match(NormalSQLParser::STRING_LITERAL);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Column_aliasContext ------------------------------------------------------------------

NormalSQLParser::Column_aliasContext::Column_aliasContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::Column_aliasContext::IDENTIFIER() {
  return getToken(NormalSQLParser::IDENTIFIER, 0);
}

tree::TerminalNode* NormalSQLParser::Column_aliasContext::STRING_LITERAL() {
  return getToken(NormalSQLParser::STRING_LITERAL, 0);
}


size_t NormalSQLParser::Column_aliasContext::getRuleIndex() const {
  return NormalSQLParser::RuleColumn_alias;
}

void NormalSQLParser::Column_aliasContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterColumn_alias(this);
}

void NormalSQLParser::Column_aliasContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitColumn_alias(this);
}


antlrcpp::Any NormalSQLParser::Column_aliasContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitColumn_alias(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Column_aliasContext* NormalSQLParser::column_alias() {
  Column_aliasContext *_localctx = _tracker.createInstance<Column_aliasContext>(_ctx, getState());
  enterRule(_localctx, 58, NormalSQLParser::RuleColumn_alias);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(681);
    _la = _input->LA(1);
    if (!(_la == NormalSQLParser::IDENTIFIER

    || _la == NormalSQLParser::STRING_LITERAL)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KeywordContext ------------------------------------------------------------------

NormalSQLParser::KeywordContext::KeywordContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_ABORT() {
  return getToken(NormalSQLParser::K_ABORT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_ACTION() {
  return getToken(NormalSQLParser::K_ACTION, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_ADD() {
  return getToken(NormalSQLParser::K_ADD, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_AFTER() {
  return getToken(NormalSQLParser::K_AFTER, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_ALL() {
  return getToken(NormalSQLParser::K_ALL, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_ALTER() {
  return getToken(NormalSQLParser::K_ALTER, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_ANALYZE() {
  return getToken(NormalSQLParser::K_ANALYZE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_AND() {
  return getToken(NormalSQLParser::K_AND, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_AS() {
  return getToken(NormalSQLParser::K_AS, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_ASC() {
  return getToken(NormalSQLParser::K_ASC, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_ATTACH() {
  return getToken(NormalSQLParser::K_ATTACH, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_AUTOINCREMENT() {
  return getToken(NormalSQLParser::K_AUTOINCREMENT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_BEFORE() {
  return getToken(NormalSQLParser::K_BEFORE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_BEGIN() {
  return getToken(NormalSQLParser::K_BEGIN, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_BETWEEN() {
  return getToken(NormalSQLParser::K_BETWEEN, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_BY() {
  return getToken(NormalSQLParser::K_BY, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_CASCADE() {
  return getToken(NormalSQLParser::K_CASCADE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_CASE() {
  return getToken(NormalSQLParser::K_CASE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_CAST() {
  return getToken(NormalSQLParser::K_CAST, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_CHECK() {
  return getToken(NormalSQLParser::K_CHECK, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_COLLATE() {
  return getToken(NormalSQLParser::K_COLLATE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_COLUMN() {
  return getToken(NormalSQLParser::K_COLUMN, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_COMMIT() {
  return getToken(NormalSQLParser::K_COMMIT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_CONFLICT() {
  return getToken(NormalSQLParser::K_CONFLICT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_CONSTRAINT() {
  return getToken(NormalSQLParser::K_CONSTRAINT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_CREATE() {
  return getToken(NormalSQLParser::K_CREATE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_CROSS() {
  return getToken(NormalSQLParser::K_CROSS, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_CURRENT_DATE() {
  return getToken(NormalSQLParser::K_CURRENT_DATE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_CURRENT_TIME() {
  return getToken(NormalSQLParser::K_CURRENT_TIME, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_CURRENT_TIMESTAMP() {
  return getToken(NormalSQLParser::K_CURRENT_TIMESTAMP, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_DATABASE() {
  return getToken(NormalSQLParser::K_DATABASE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_DEFAULT() {
  return getToken(NormalSQLParser::K_DEFAULT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_DEFERRABLE() {
  return getToken(NormalSQLParser::K_DEFERRABLE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_DEFERRED() {
  return getToken(NormalSQLParser::K_DEFERRED, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_DELETE() {
  return getToken(NormalSQLParser::K_DELETE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_DESC() {
  return getToken(NormalSQLParser::K_DESC, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_DETACH() {
  return getToken(NormalSQLParser::K_DETACH, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_DISTINCT() {
  return getToken(NormalSQLParser::K_DISTINCT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_DROP() {
  return getToken(NormalSQLParser::K_DROP, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_EACH() {
  return getToken(NormalSQLParser::K_EACH, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_ELSE() {
  return getToken(NormalSQLParser::K_ELSE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_END() {
  return getToken(NormalSQLParser::K_END, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_ESCAPE() {
  return getToken(NormalSQLParser::K_ESCAPE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_EXCEPT() {
  return getToken(NormalSQLParser::K_EXCEPT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_EXCLUSIVE() {
  return getToken(NormalSQLParser::K_EXCLUSIVE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_EXISTS() {
  return getToken(NormalSQLParser::K_EXISTS, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_EXPLAIN() {
  return getToken(NormalSQLParser::K_EXPLAIN, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_FAIL() {
  return getToken(NormalSQLParser::K_FAIL, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_FOR() {
  return getToken(NormalSQLParser::K_FOR, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_FOREIGN() {
  return getToken(NormalSQLParser::K_FOREIGN, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_FROM() {
  return getToken(NormalSQLParser::K_FROM, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_FULL() {
  return getToken(NormalSQLParser::K_FULL, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_GLOB() {
  return getToken(NormalSQLParser::K_GLOB, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_GROUP() {
  return getToken(NormalSQLParser::K_GROUP, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_HAVING() {
  return getToken(NormalSQLParser::K_HAVING, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_IF() {
  return getToken(NormalSQLParser::K_IF, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_IGNORE() {
  return getToken(NormalSQLParser::K_IGNORE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_IMMEDIATE() {
  return getToken(NormalSQLParser::K_IMMEDIATE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_IN() {
  return getToken(NormalSQLParser::K_IN, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_INDEX() {
  return getToken(NormalSQLParser::K_INDEX, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_INDEXED() {
  return getToken(NormalSQLParser::K_INDEXED, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_INITIALLY() {
  return getToken(NormalSQLParser::K_INITIALLY, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_INNER() {
  return getToken(NormalSQLParser::K_INNER, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_INSERT() {
  return getToken(NormalSQLParser::K_INSERT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_INSTEAD() {
  return getToken(NormalSQLParser::K_INSTEAD, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_INTERSECT() {
  return getToken(NormalSQLParser::K_INTERSECT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_INTO() {
  return getToken(NormalSQLParser::K_INTO, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_IS() {
  return getToken(NormalSQLParser::K_IS, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_ISNULL() {
  return getToken(NormalSQLParser::K_ISNULL, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_JOIN() {
  return getToken(NormalSQLParser::K_JOIN, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_KEY() {
  return getToken(NormalSQLParser::K_KEY, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_LEFT() {
  return getToken(NormalSQLParser::K_LEFT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_LIKE() {
  return getToken(NormalSQLParser::K_LIKE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_LIMIT() {
  return getToken(NormalSQLParser::K_LIMIT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_MATCH() {
  return getToken(NormalSQLParser::K_MATCH, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_NATURAL() {
  return getToken(NormalSQLParser::K_NATURAL, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_NO() {
  return getToken(NormalSQLParser::K_NO, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_NOT() {
  return getToken(NormalSQLParser::K_NOT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_NOTNULL() {
  return getToken(NormalSQLParser::K_NOTNULL, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_NULL() {
  return getToken(NormalSQLParser::K_NULL, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_OF() {
  return getToken(NormalSQLParser::K_OF, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_OFFSET() {
  return getToken(NormalSQLParser::K_OFFSET, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_ON() {
  return getToken(NormalSQLParser::K_ON, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_OR() {
  return getToken(NormalSQLParser::K_OR, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_ORDER() {
  return getToken(NormalSQLParser::K_ORDER, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_OUTER() {
  return getToken(NormalSQLParser::K_OUTER, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_PLAN() {
  return getToken(NormalSQLParser::K_PLAN, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_PRAGMA() {
  return getToken(NormalSQLParser::K_PRAGMA, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_PRIMARY() {
  return getToken(NormalSQLParser::K_PRIMARY, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_QUERY() {
  return getToken(NormalSQLParser::K_QUERY, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_RAISE() {
  return getToken(NormalSQLParser::K_RAISE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_RECURSIVE() {
  return getToken(NormalSQLParser::K_RECURSIVE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_REFERENCES() {
  return getToken(NormalSQLParser::K_REFERENCES, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_REGEXP() {
  return getToken(NormalSQLParser::K_REGEXP, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_REINDEX() {
  return getToken(NormalSQLParser::K_REINDEX, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_RELEASE() {
  return getToken(NormalSQLParser::K_RELEASE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_RENAME() {
  return getToken(NormalSQLParser::K_RENAME, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_REPLACE() {
  return getToken(NormalSQLParser::K_REPLACE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_RESTRICT() {
  return getToken(NormalSQLParser::K_RESTRICT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_RIGHT() {
  return getToken(NormalSQLParser::K_RIGHT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_ROLLBACK() {
  return getToken(NormalSQLParser::K_ROLLBACK, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_ROW() {
  return getToken(NormalSQLParser::K_ROW, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_SAVEPOINT() {
  return getToken(NormalSQLParser::K_SAVEPOINT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_SELECT() {
  return getToken(NormalSQLParser::K_SELECT, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_SET() {
  return getToken(NormalSQLParser::K_SET, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_TABLE() {
  return getToken(NormalSQLParser::K_TABLE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_TEMP() {
  return getToken(NormalSQLParser::K_TEMP, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_TEMPORARY() {
  return getToken(NormalSQLParser::K_TEMPORARY, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_THEN() {
  return getToken(NormalSQLParser::K_THEN, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_TO() {
  return getToken(NormalSQLParser::K_TO, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_TRANSACTION() {
  return getToken(NormalSQLParser::K_TRANSACTION, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_TRIGGER() {
  return getToken(NormalSQLParser::K_TRIGGER, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_UNION() {
  return getToken(NormalSQLParser::K_UNION, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_UNIQUE() {
  return getToken(NormalSQLParser::K_UNIQUE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_UPDATE() {
  return getToken(NormalSQLParser::K_UPDATE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_USING() {
  return getToken(NormalSQLParser::K_USING, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_VACUUM() {
  return getToken(NormalSQLParser::K_VACUUM, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_VALUES() {
  return getToken(NormalSQLParser::K_VALUES, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_VIEW() {
  return getToken(NormalSQLParser::K_VIEW, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_VIRTUAL() {
  return getToken(NormalSQLParser::K_VIRTUAL, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_WHEN() {
  return getToken(NormalSQLParser::K_WHEN, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_WHERE() {
  return getToken(NormalSQLParser::K_WHERE, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_WITH() {
  return getToken(NormalSQLParser::K_WITH, 0);
}

tree::TerminalNode* NormalSQLParser::KeywordContext::K_WITHOUT() {
  return getToken(NormalSQLParser::K_WITHOUT, 0);
}


size_t NormalSQLParser::KeywordContext::getRuleIndex() const {
  return NormalSQLParser::RuleKeyword;
}

void NormalSQLParser::KeywordContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterKeyword(this);
}

void NormalSQLParser::KeywordContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitKeyword(this);
}


antlrcpp::Any NormalSQLParser::KeywordContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitKeyword(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::KeywordContext* NormalSQLParser::keyword() {
  KeywordContext *_localctx = _tracker.createInstance<KeywordContext>(_ctx, getState());
  enterRule(_localctx, 60, NormalSQLParser::RuleKeyword);
  size_t _la = 0;

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(683);
    _la = _input->LA(1);
    if (!(((((_la - 26) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 26)) & ((1ULL << (NormalSQLParser::K_ABORT - 26))
      | (1ULL << (NormalSQLParser::K_ACTION - 26))
      | (1ULL << (NormalSQLParser::K_ADD - 26))
      | (1ULL << (NormalSQLParser::K_AFTER - 26))
      | (1ULL << (NormalSQLParser::K_ALL - 26))
      | (1ULL << (NormalSQLParser::K_ALTER - 26))
      | (1ULL << (NormalSQLParser::K_ANALYZE - 26))
      | (1ULL << (NormalSQLParser::K_AND - 26))
      | (1ULL << (NormalSQLParser::K_AS - 26))
      | (1ULL << (NormalSQLParser::K_ASC - 26))
      | (1ULL << (NormalSQLParser::K_ATTACH - 26))
      | (1ULL << (NormalSQLParser::K_AUTOINCREMENT - 26))
      | (1ULL << (NormalSQLParser::K_BEFORE - 26))
      | (1ULL << (NormalSQLParser::K_BEGIN - 26))
      | (1ULL << (NormalSQLParser::K_BETWEEN - 26))
      | (1ULL << (NormalSQLParser::K_BY - 26))
      | (1ULL << (NormalSQLParser::K_CASCADE - 26))
      | (1ULL << (NormalSQLParser::K_CASE - 26))
      | (1ULL << (NormalSQLParser::K_CAST - 26))
      | (1ULL << (NormalSQLParser::K_CHECK - 26))
      | (1ULL << (NormalSQLParser::K_COLLATE - 26))
      | (1ULL << (NormalSQLParser::K_COLUMN - 26))
      | (1ULL << (NormalSQLParser::K_COMMIT - 26))
      | (1ULL << (NormalSQLParser::K_CONFLICT - 26))
      | (1ULL << (NormalSQLParser::K_CONSTRAINT - 26))
      | (1ULL << (NormalSQLParser::K_CREATE - 26))
      | (1ULL << (NormalSQLParser::K_CROSS - 26))
      | (1ULL << (NormalSQLParser::K_CURRENT_DATE - 26))
      | (1ULL << (NormalSQLParser::K_CURRENT_TIME - 26))
      | (1ULL << (NormalSQLParser::K_CURRENT_TIMESTAMP - 26))
      | (1ULL << (NormalSQLParser::K_DATABASE - 26))
      | (1ULL << (NormalSQLParser::K_DEFAULT - 26))
      | (1ULL << (NormalSQLParser::K_DEFERRABLE - 26))
      | (1ULL << (NormalSQLParser::K_DEFERRED - 26))
      | (1ULL << (NormalSQLParser::K_DELETE - 26))
      | (1ULL << (NormalSQLParser::K_DESC - 26))
      | (1ULL << (NormalSQLParser::K_DETACH - 26))
      | (1ULL << (NormalSQLParser::K_DISTINCT - 26))
      | (1ULL << (NormalSQLParser::K_DROP - 26))
      | (1ULL << (NormalSQLParser::K_EACH - 26))
      | (1ULL << (NormalSQLParser::K_ELSE - 26))
      | (1ULL << (NormalSQLParser::K_END - 26))
      | (1ULL << (NormalSQLParser::K_ESCAPE - 26))
      | (1ULL << (NormalSQLParser::K_EXCEPT - 26))
      | (1ULL << (NormalSQLParser::K_EXCLUSIVE - 26))
      | (1ULL << (NormalSQLParser::K_EXISTS - 26))
      | (1ULL << (NormalSQLParser::K_EXPLAIN - 26))
      | (1ULL << (NormalSQLParser::K_FAIL - 26))
      | (1ULL << (NormalSQLParser::K_FOR - 26))
      | (1ULL << (NormalSQLParser::K_FOREIGN - 26))
      | (1ULL << (NormalSQLParser::K_FROM - 26))
      | (1ULL << (NormalSQLParser::K_FULL - 26))
      | (1ULL << (NormalSQLParser::K_GLOB - 26))
      | (1ULL << (NormalSQLParser::K_GROUP - 26))
      | (1ULL << (NormalSQLParser::K_HAVING - 26))
      | (1ULL << (NormalSQLParser::K_IF - 26))
      | (1ULL << (NormalSQLParser::K_IGNORE - 26))
      | (1ULL << (NormalSQLParser::K_IMMEDIATE - 26))
      | (1ULL << (NormalSQLParser::K_IN - 26))
      | (1ULL << (NormalSQLParser::K_INDEX - 26))
      | (1ULL << (NormalSQLParser::K_INDEXED - 26))
      | (1ULL << (NormalSQLParser::K_INITIALLY - 26))
      | (1ULL << (NormalSQLParser::K_INNER - 26))
      | (1ULL << (NormalSQLParser::K_INSERT - 26)))) != 0) || ((((_la - 90) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 90)) & ((1ULL << (NormalSQLParser::K_INSTEAD - 90))
      | (1ULL << (NormalSQLParser::K_INTERSECT - 90))
      | (1ULL << (NormalSQLParser::K_INTO - 90))
      | (1ULL << (NormalSQLParser::K_IS - 90))
      | (1ULL << (NormalSQLParser::K_ISNULL - 90))
      | (1ULL << (NormalSQLParser::K_JOIN - 90))
      | (1ULL << (NormalSQLParser::K_KEY - 90))
      | (1ULL << (NormalSQLParser::K_LEFT - 90))
      | (1ULL << (NormalSQLParser::K_LIKE - 90))
      | (1ULL << (NormalSQLParser::K_LIMIT - 90))
      | (1ULL << (NormalSQLParser::K_MATCH - 90))
      | (1ULL << (NormalSQLParser::K_NATURAL - 90))
      | (1ULL << (NormalSQLParser::K_NO - 90))
      | (1ULL << (NormalSQLParser::K_NOT - 90))
      | (1ULL << (NormalSQLParser::K_NOTNULL - 90))
      | (1ULL << (NormalSQLParser::K_NULL - 90))
      | (1ULL << (NormalSQLParser::K_OF - 90))
      | (1ULL << (NormalSQLParser::K_OFFSET - 90))
      | (1ULL << (NormalSQLParser::K_ON - 90))
      | (1ULL << (NormalSQLParser::K_OR - 90))
      | (1ULL << (NormalSQLParser::K_ORDER - 90))
      | (1ULL << (NormalSQLParser::K_OUTER - 90))
      | (1ULL << (NormalSQLParser::K_PLAN - 90))
      | (1ULL << (NormalSQLParser::K_PRAGMA - 90))
      | (1ULL << (NormalSQLParser::K_PRIMARY - 90))
      | (1ULL << (NormalSQLParser::K_QUERY - 90))
      | (1ULL << (NormalSQLParser::K_RAISE - 90))
      | (1ULL << (NormalSQLParser::K_RECURSIVE - 90))
      | (1ULL << (NormalSQLParser::K_REFERENCES - 90))
      | (1ULL << (NormalSQLParser::K_REGEXP - 90))
      | (1ULL << (NormalSQLParser::K_REINDEX - 90))
      | (1ULL << (NormalSQLParser::K_RELEASE - 90))
      | (1ULL << (NormalSQLParser::K_RENAME - 90))
      | (1ULL << (NormalSQLParser::K_REPLACE - 90))
      | (1ULL << (NormalSQLParser::K_RESTRICT - 90))
      | (1ULL << (NormalSQLParser::K_RIGHT - 90))
      | (1ULL << (NormalSQLParser::K_ROLLBACK - 90))
      | (1ULL << (NormalSQLParser::K_ROW - 90))
      | (1ULL << (NormalSQLParser::K_SAVEPOINT - 90))
      | (1ULL << (NormalSQLParser::K_SELECT - 90))
      | (1ULL << (NormalSQLParser::K_SET - 90))
      | (1ULL << (NormalSQLParser::K_TABLE - 90))
      | (1ULL << (NormalSQLParser::K_TEMP - 90))
      | (1ULL << (NormalSQLParser::K_TEMPORARY - 90))
      | (1ULL << (NormalSQLParser::K_THEN - 90))
      | (1ULL << (NormalSQLParser::K_TO - 90))
      | (1ULL << (NormalSQLParser::K_TRANSACTION - 90))
      | (1ULL << (NormalSQLParser::K_TRIGGER - 90))
      | (1ULL << (NormalSQLParser::K_UNION - 90))
      | (1ULL << (NormalSQLParser::K_UNIQUE - 90))
      | (1ULL << (NormalSQLParser::K_UPDATE - 90))
      | (1ULL << (NormalSQLParser::K_USING - 90))
      | (1ULL << (NormalSQLParser::K_VACUUM - 90))
      | (1ULL << (NormalSQLParser::K_VALUES - 90))
      | (1ULL << (NormalSQLParser::K_VIEW - 90))
      | (1ULL << (NormalSQLParser::K_VIRTUAL - 90))
      | (1ULL << (NormalSQLParser::K_WHEN - 90))
      | (1ULL << (NormalSQLParser::K_WHERE - 90))
      | (1ULL << (NormalSQLParser::K_WITH - 90))
      | (1ULL << (NormalSQLParser::K_WITHOUT - 90)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NameContext ------------------------------------------------------------------

NormalSQLParser::NameContext::NameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::NameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::NameContext::getRuleIndex() const {
  return NormalSQLParser::RuleName;
}

void NormalSQLParser::NameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterName(this);
}

void NormalSQLParser::NameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitName(this);
}


antlrcpp::Any NormalSQLParser::NameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitName(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::NameContext* NormalSQLParser::name() {
  NameContext *_localctx = _tracker.createInstance<NameContext>(_ctx, getState());
  enterRule(_localctx, 62, NormalSQLParser::RuleName);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(685);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Function_nameContext ------------------------------------------------------------------

NormalSQLParser::Function_nameContext::Function_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::Function_nameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::Function_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleFunction_name;
}

void NormalSQLParser::Function_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFunction_name(this);
}

void NormalSQLParser::Function_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFunction_name(this);
}


antlrcpp::Any NormalSQLParser::Function_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitFunction_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Function_nameContext* NormalSQLParser::function_name() {
  Function_nameContext *_localctx = _tracker.createInstance<Function_nameContext>(_ctx, getState());
  enterRule(_localctx, 64, NormalSQLParser::RuleFunction_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(687);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Database_nameContext ------------------------------------------------------------------

NormalSQLParser::Database_nameContext::Database_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::Database_nameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::Database_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleDatabase_name;
}

void NormalSQLParser::Database_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterDatabase_name(this);
}

void NormalSQLParser::Database_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitDatabase_name(this);
}


antlrcpp::Any NormalSQLParser::Database_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitDatabase_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Database_nameContext* NormalSQLParser::database_name() {
  Database_nameContext *_localctx = _tracker.createInstance<Database_nameContext>(_ctx, getState());
  enterRule(_localctx, 66, NormalSQLParser::RuleDatabase_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(689);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Table_nameContext ------------------------------------------------------------------

NormalSQLParser::Table_nameContext::Table_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::Table_nameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::Table_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleTable_name;
}

void NormalSQLParser::Table_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTable_name(this);
}

void NormalSQLParser::Table_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTable_name(this);
}


antlrcpp::Any NormalSQLParser::Table_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitTable_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Table_nameContext* NormalSQLParser::table_name() {
  Table_nameContext *_localctx = _tracker.createInstance<Table_nameContext>(_ctx, getState());
  enterRule(_localctx, 68, NormalSQLParser::RuleTable_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(691);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Table_or_index_nameContext ------------------------------------------------------------------

NormalSQLParser::Table_or_index_nameContext::Table_or_index_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::Table_or_index_nameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::Table_or_index_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleTable_or_index_name;
}

void NormalSQLParser::Table_or_index_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTable_or_index_name(this);
}

void NormalSQLParser::Table_or_index_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTable_or_index_name(this);
}


antlrcpp::Any NormalSQLParser::Table_or_index_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitTable_or_index_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Table_or_index_nameContext* NormalSQLParser::table_or_index_name() {
  Table_or_index_nameContext *_localctx = _tracker.createInstance<Table_or_index_nameContext>(_ctx, getState());
  enterRule(_localctx, 70, NormalSQLParser::RuleTable_or_index_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(693);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- New_table_nameContext ------------------------------------------------------------------

NormalSQLParser::New_table_nameContext::New_table_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::New_table_nameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::New_table_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleNew_table_name;
}

void NormalSQLParser::New_table_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNew_table_name(this);
}

void NormalSQLParser::New_table_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNew_table_name(this);
}


antlrcpp::Any NormalSQLParser::New_table_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitNew_table_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::New_table_nameContext* NormalSQLParser::new_table_name() {
  New_table_nameContext *_localctx = _tracker.createInstance<New_table_nameContext>(_ctx, getState());
  enterRule(_localctx, 72, NormalSQLParser::RuleNew_table_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(695);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Column_nameContext ------------------------------------------------------------------

NormalSQLParser::Column_nameContext::Column_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::Column_nameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::Column_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleColumn_name;
}

void NormalSQLParser::Column_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterColumn_name(this);
}

void NormalSQLParser::Column_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitColumn_name(this);
}


antlrcpp::Any NormalSQLParser::Column_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitColumn_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Column_nameContext* NormalSQLParser::column_name() {
  Column_nameContext *_localctx = _tracker.createInstance<Column_nameContext>(_ctx, getState());
  enterRule(_localctx, 74, NormalSQLParser::RuleColumn_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(697);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Collation_nameContext ------------------------------------------------------------------

NormalSQLParser::Collation_nameContext::Collation_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::Collation_nameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::Collation_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleCollation_name;
}

void NormalSQLParser::Collation_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterCollation_name(this);
}

void NormalSQLParser::Collation_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitCollation_name(this);
}


antlrcpp::Any NormalSQLParser::Collation_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitCollation_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Collation_nameContext* NormalSQLParser::collation_name() {
  Collation_nameContext *_localctx = _tracker.createInstance<Collation_nameContext>(_ctx, getState());
  enterRule(_localctx, 76, NormalSQLParser::RuleCollation_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(699);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Foreign_tableContext ------------------------------------------------------------------

NormalSQLParser::Foreign_tableContext::Foreign_tableContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::Foreign_tableContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::Foreign_tableContext::getRuleIndex() const {
  return NormalSQLParser::RuleForeign_table;
}

void NormalSQLParser::Foreign_tableContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterForeign_table(this);
}

void NormalSQLParser::Foreign_tableContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitForeign_table(this);
}


antlrcpp::Any NormalSQLParser::Foreign_tableContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitForeign_table(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Foreign_tableContext* NormalSQLParser::foreign_table() {
  Foreign_tableContext *_localctx = _tracker.createInstance<Foreign_tableContext>(_ctx, getState());
  enterRule(_localctx, 78, NormalSQLParser::RuleForeign_table);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(701);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Index_nameContext ------------------------------------------------------------------

NormalSQLParser::Index_nameContext::Index_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::Index_nameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::Index_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleIndex_name;
}

void NormalSQLParser::Index_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterIndex_name(this);
}

void NormalSQLParser::Index_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitIndex_name(this);
}


antlrcpp::Any NormalSQLParser::Index_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitIndex_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Index_nameContext* NormalSQLParser::index_name() {
  Index_nameContext *_localctx = _tracker.createInstance<Index_nameContext>(_ctx, getState());
  enterRule(_localctx, 80, NormalSQLParser::RuleIndex_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(703);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Trigger_nameContext ------------------------------------------------------------------

NormalSQLParser::Trigger_nameContext::Trigger_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::Trigger_nameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::Trigger_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleTrigger_name;
}

void NormalSQLParser::Trigger_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTrigger_name(this);
}

void NormalSQLParser::Trigger_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTrigger_name(this);
}


antlrcpp::Any NormalSQLParser::Trigger_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitTrigger_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Trigger_nameContext* NormalSQLParser::trigger_name() {
  Trigger_nameContext *_localctx = _tracker.createInstance<Trigger_nameContext>(_ctx, getState());
  enterRule(_localctx, 82, NormalSQLParser::RuleTrigger_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(705);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- View_nameContext ------------------------------------------------------------------

NormalSQLParser::View_nameContext::View_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::View_nameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::View_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleView_name;
}

void NormalSQLParser::View_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterView_name(this);
}

void NormalSQLParser::View_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitView_name(this);
}


antlrcpp::Any NormalSQLParser::View_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitView_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::View_nameContext* NormalSQLParser::view_name() {
  View_nameContext *_localctx = _tracker.createInstance<View_nameContext>(_ctx, getState());
  enterRule(_localctx, 84, NormalSQLParser::RuleView_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(707);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Module_nameContext ------------------------------------------------------------------

NormalSQLParser::Module_nameContext::Module_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::Module_nameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::Module_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleModule_name;
}

void NormalSQLParser::Module_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterModule_name(this);
}

void NormalSQLParser::Module_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitModule_name(this);
}


antlrcpp::Any NormalSQLParser::Module_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitModule_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Module_nameContext* NormalSQLParser::module_name() {
  Module_nameContext *_localctx = _tracker.createInstance<Module_nameContext>(_ctx, getState());
  enterRule(_localctx, 86, NormalSQLParser::RuleModule_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(709);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Pragma_nameContext ------------------------------------------------------------------

NormalSQLParser::Pragma_nameContext::Pragma_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::Pragma_nameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::Pragma_nameContext::getRuleIndex() const {
  return NormalSQLParser::RulePragma_name;
}

void NormalSQLParser::Pragma_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPragma_name(this);
}

void NormalSQLParser::Pragma_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPragma_name(this);
}


antlrcpp::Any NormalSQLParser::Pragma_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitPragma_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Pragma_nameContext* NormalSQLParser::pragma_name() {
  Pragma_nameContext *_localctx = _tracker.createInstance<Pragma_nameContext>(_ctx, getState());
  enterRule(_localctx, 88, NormalSQLParser::RulePragma_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(711);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Savepoint_nameContext ------------------------------------------------------------------

NormalSQLParser::Savepoint_nameContext::Savepoint_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::Savepoint_nameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::Savepoint_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleSavepoint_name;
}

void NormalSQLParser::Savepoint_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSavepoint_name(this);
}

void NormalSQLParser::Savepoint_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSavepoint_name(this);
}


antlrcpp::Any NormalSQLParser::Savepoint_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitSavepoint_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Savepoint_nameContext* NormalSQLParser::savepoint_name() {
  Savepoint_nameContext *_localctx = _tracker.createInstance<Savepoint_nameContext>(_ctx, getState());
  enterRule(_localctx, 90, NormalSQLParser::RuleSavepoint_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(713);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Table_aliasContext ------------------------------------------------------------------

NormalSQLParser::Table_aliasContext::Table_aliasContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::Table_aliasContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::Table_aliasContext::getRuleIndex() const {
  return NormalSQLParser::RuleTable_alias;
}

void NormalSQLParser::Table_aliasContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTable_alias(this);
}

void NormalSQLParser::Table_aliasContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTable_alias(this);
}


antlrcpp::Any NormalSQLParser::Table_aliasContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitTable_alias(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Table_aliasContext* NormalSQLParser::table_alias() {
  Table_aliasContext *_localctx = _tracker.createInstance<Table_aliasContext>(_ctx, getState());
  enterRule(_localctx, 92, NormalSQLParser::RuleTable_alias);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(715);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Transaction_nameContext ------------------------------------------------------------------

NormalSQLParser::Transaction_nameContext::Transaction_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

NormalSQLParser::Any_nameContext* NormalSQLParser::Transaction_nameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}


size_t NormalSQLParser::Transaction_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleTransaction_name;
}

void NormalSQLParser::Transaction_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTransaction_name(this);
}

void NormalSQLParser::Transaction_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTransaction_name(this);
}


antlrcpp::Any NormalSQLParser::Transaction_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitTransaction_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Transaction_nameContext* NormalSQLParser::transaction_name() {
  Transaction_nameContext *_localctx = _tracker.createInstance<Transaction_nameContext>(_ctx, getState());
  enterRule(_localctx, 94, NormalSQLParser::RuleTransaction_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(717);
    any_name();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Any_nameContext ------------------------------------------------------------------

NormalSQLParser::Any_nameContext::Any_nameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* NormalSQLParser::Any_nameContext::IDENTIFIER() {
  return getToken(NormalSQLParser::IDENTIFIER, 0);
}

NormalSQLParser::KeywordContext* NormalSQLParser::Any_nameContext::keyword() {
  return getRuleContext<NormalSQLParser::KeywordContext>(0);
}

tree::TerminalNode* NormalSQLParser::Any_nameContext::STRING_LITERAL() {
  return getToken(NormalSQLParser::STRING_LITERAL, 0);
}

tree::TerminalNode* NormalSQLParser::Any_nameContext::OPEN_PAR() {
  return getToken(NormalSQLParser::OPEN_PAR, 0);
}

NormalSQLParser::Any_nameContext* NormalSQLParser::Any_nameContext::any_name() {
  return getRuleContext<NormalSQLParser::Any_nameContext>(0);
}

tree::TerminalNode* NormalSQLParser::Any_nameContext::CLOSE_PAR() {
  return getToken(NormalSQLParser::CLOSE_PAR, 0);
}


size_t NormalSQLParser::Any_nameContext::getRuleIndex() const {
  return NormalSQLParser::RuleAny_name;
}

void NormalSQLParser::Any_nameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAny_name(this);
}

void NormalSQLParser::Any_nameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<NormalSQLListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitAny_name(this);
}


antlrcpp::Any NormalSQLParser::Any_nameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<NormalSQLVisitor*>(visitor))
    return parserVisitor->visitAny_name(this);
  else
    return visitor->visitChildren(this);
}

NormalSQLParser::Any_nameContext* NormalSQLParser::any_name() {
  Any_nameContext *_localctx = _tracker.createInstance<Any_nameContext>(_ctx, getState());
  enterRule(_localctx, 96, NormalSQLParser::RuleAny_name);

  auto onExit = finally([=] {
    exitRule();
  });
  try {
    setState(726);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case NormalSQLParser::IDENTIFIER: {
        enterOuterAlt(_localctx, 1);
        setState(719);
        match(NormalSQLParser::IDENTIFIER);
        break;
      }

      case NormalSQLParser::K_ABORT:
      case NormalSQLParser::K_ACTION:
      case NormalSQLParser::K_ADD:
      case NormalSQLParser::K_AFTER:
      case NormalSQLParser::K_ALL:
      case NormalSQLParser::K_ALTER:
      case NormalSQLParser::K_ANALYZE:
      case NormalSQLParser::K_AND:
      case NormalSQLParser::K_AS:
      case NormalSQLParser::K_ASC:
      case NormalSQLParser::K_ATTACH:
      case NormalSQLParser::K_AUTOINCREMENT:
      case NormalSQLParser::K_BEFORE:
      case NormalSQLParser::K_BEGIN:
      case NormalSQLParser::K_BETWEEN:
      case NormalSQLParser::K_BY:
      case NormalSQLParser::K_CASCADE:
      case NormalSQLParser::K_CASE:
      case NormalSQLParser::K_CAST:
      case NormalSQLParser::K_CHECK:
      case NormalSQLParser::K_COLLATE:
      case NormalSQLParser::K_COLUMN:
      case NormalSQLParser::K_COMMIT:
      case NormalSQLParser::K_CONFLICT:
      case NormalSQLParser::K_CONSTRAINT:
      case NormalSQLParser::K_CREATE:
      case NormalSQLParser::K_CROSS:
      case NormalSQLParser::K_CURRENT_DATE:
      case NormalSQLParser::K_CURRENT_TIME:
      case NormalSQLParser::K_CURRENT_TIMESTAMP:
      case NormalSQLParser::K_DATABASE:
      case NormalSQLParser::K_DEFAULT:
      case NormalSQLParser::K_DEFERRABLE:
      case NormalSQLParser::K_DEFERRED:
      case NormalSQLParser::K_DELETE:
      case NormalSQLParser::K_DESC:
      case NormalSQLParser::K_DETACH:
      case NormalSQLParser::K_DISTINCT:
      case NormalSQLParser::K_DROP:
      case NormalSQLParser::K_EACH:
      case NormalSQLParser::K_ELSE:
      case NormalSQLParser::K_END:
      case NormalSQLParser::K_ESCAPE:
      case NormalSQLParser::K_EXCEPT:
      case NormalSQLParser::K_EXCLUSIVE:
      case NormalSQLParser::K_EXISTS:
      case NormalSQLParser::K_EXPLAIN:
      case NormalSQLParser::K_FAIL:
      case NormalSQLParser::K_FOR:
      case NormalSQLParser::K_FOREIGN:
      case NormalSQLParser::K_FROM:
      case NormalSQLParser::K_FULL:
      case NormalSQLParser::K_GLOB:
      case NormalSQLParser::K_GROUP:
      case NormalSQLParser::K_HAVING:
      case NormalSQLParser::K_IF:
      case NormalSQLParser::K_IGNORE:
      case NormalSQLParser::K_IMMEDIATE:
      case NormalSQLParser::K_IN:
      case NormalSQLParser::K_INDEX:
      case NormalSQLParser::K_INDEXED:
      case NormalSQLParser::K_INITIALLY:
      case NormalSQLParser::K_INNER:
      case NormalSQLParser::K_INSERT:
      case NormalSQLParser::K_INSTEAD:
      case NormalSQLParser::K_INTERSECT:
      case NormalSQLParser::K_INTO:
      case NormalSQLParser::K_IS:
      case NormalSQLParser::K_ISNULL:
      case NormalSQLParser::K_JOIN:
      case NormalSQLParser::K_KEY:
      case NormalSQLParser::K_LEFT:
      case NormalSQLParser::K_LIKE:
      case NormalSQLParser::K_LIMIT:
      case NormalSQLParser::K_MATCH:
      case NormalSQLParser::K_NATURAL:
      case NormalSQLParser::K_NO:
      case NormalSQLParser::K_NOT:
      case NormalSQLParser::K_NOTNULL:
      case NormalSQLParser::K_NULL:
      case NormalSQLParser::K_OF:
      case NormalSQLParser::K_OFFSET:
      case NormalSQLParser::K_ON:
      case NormalSQLParser::K_OR:
      case NormalSQLParser::K_ORDER:
      case NormalSQLParser::K_OUTER:
      case NormalSQLParser::K_PLAN:
      case NormalSQLParser::K_PRAGMA:
      case NormalSQLParser::K_PRIMARY:
      case NormalSQLParser::K_QUERY:
      case NormalSQLParser::K_RAISE:
      case NormalSQLParser::K_RECURSIVE:
      case NormalSQLParser::K_REFERENCES:
      case NormalSQLParser::K_REGEXP:
      case NormalSQLParser::K_REINDEX:
      case NormalSQLParser::K_RELEASE:
      case NormalSQLParser::K_RENAME:
      case NormalSQLParser::K_REPLACE:
      case NormalSQLParser::K_RESTRICT:
      case NormalSQLParser::K_RIGHT:
      case NormalSQLParser::K_ROLLBACK:
      case NormalSQLParser::K_ROW:
      case NormalSQLParser::K_SAVEPOINT:
      case NormalSQLParser::K_SELECT:
      case NormalSQLParser::K_SET:
      case NormalSQLParser::K_TABLE:
      case NormalSQLParser::K_TEMP:
      case NormalSQLParser::K_TEMPORARY:
      case NormalSQLParser::K_THEN:
      case NormalSQLParser::K_TO:
      case NormalSQLParser::K_TRANSACTION:
      case NormalSQLParser::K_TRIGGER:
      case NormalSQLParser::K_UNION:
      case NormalSQLParser::K_UNIQUE:
      case NormalSQLParser::K_UPDATE:
      case NormalSQLParser::K_USING:
      case NormalSQLParser::K_VACUUM:
      case NormalSQLParser::K_VALUES:
      case NormalSQLParser::K_VIEW:
      case NormalSQLParser::K_VIRTUAL:
      case NormalSQLParser::K_WHEN:
      case NormalSQLParser::K_WHERE:
      case NormalSQLParser::K_WITH:
      case NormalSQLParser::K_WITHOUT: {
        enterOuterAlt(_localctx, 2);
        setState(720);
        keyword();
        break;
      }

      case NormalSQLParser::STRING_LITERAL: {
        enterOuterAlt(_localctx, 3);
        setState(721);
        match(NormalSQLParser::STRING_LITERAL);
        break;
      }

      case NormalSQLParser::OPEN_PAR: {
        enterOuterAlt(_localctx, 4);
        setState(722);
        match(NormalSQLParser::OPEN_PAR);
        setState(723);
        any_name();
        setState(724);
        match(NormalSQLParser::CLOSE_PAR);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

bool NormalSQLParser::sempred(RuleContext *context, size_t ruleIndex, size_t predicateIndex) {
  switch (ruleIndex) {
    case 10: return exprSempred(dynamic_cast<ExprContext *>(context), predicateIndex);

  default:
    break;
  }
  return true;
}

bool NormalSQLParser::exprSempred(ExprContext *_localctx, size_t predicateIndex) {
  switch (predicateIndex) {
    case 0: return precpred(_ctx, 21);
    case 1: return precpred(_ctx, 20);
    case 2: return precpred(_ctx, 19);
    case 3: return precpred(_ctx, 18);
    case 4: return precpred(_ctx, 17);
    case 5: return precpred(_ctx, 16);
    case 6: return precpred(_ctx, 9);
    case 7: return precpred(_ctx, 4);
    case 8: return precpred(_ctx, 3);
    case 9: return precpred(_ctx, 2);
    case 10: return precpred(_ctx, 1);
    case 11: return precpred(_ctx, 12);
    case 12: return precpred(_ctx, 11);
    case 13: return precpred(_ctx, 10);
    case 14: return precpred(_ctx, 8);

  default:
    break;
  }
  return true;
}

// Static vars and initialization.
std::vector<dfa::DFA> NormalSQLParser::_decisionToDFA;
atn::PredictionContextCache NormalSQLParser::_sharedContextCache;

// We own the ATN which in turn owns the ATN states.
atn::ATN NormalSQLParser::_atn;
std::vector<uint16_t> NormalSQLParser::_serializedATN;

std::vector<std::string> NormalSQLParser::_ruleNames = {
  "parse", "error", "sql_stmt_list", "sql_stmt", "select_stmt", "select_core", 
  "from_clause", "where_clause", "groupBy_clause", "type_name", "expr", 
  "raise_function", "indexed_column", "with_clause", "qualified_table_name", 
  "ordering_term", "pragma_value", "common_table_expression", "result_column", 
  "table_or_subquery", "join_clause", "join_operator", "join_constraint", 
  "compound_operator", "cte_table_name", "signed_number", "literal_value", 
  "unary_operator", "error_message", "column_alias", "keyword", "name", 
  "function_name", "database_name", "table_name", "table_or_index_name", 
  "new_table_name", "column_name", "collation_name", "foreign_table", "index_name", 
  "trigger_name", "view_name", "module_name", "pragma_name", "savepoint_name", 
  "table_alias", "transaction_name", "any_name"
};

std::vector<std::string> NormalSQLParser::_literalNames = {
  "", "'&&'", "';'", "'.'", "'('", "')'", "','", "'='", "'*'", "'+'", "'-'", 
  "'~'", "'||'", "'/'", "'%'", "'<<'", "'>>'", "'&'", "'|'", "'<'", "'<='", 
  "'>'", "'>='", "'=='", "'!='", "'<>'"
};

std::vector<std::string> NormalSQLParser::_symbolicNames = {
  "", "", "SCOL", "DOT", "OPEN_PAR", "CLOSE_PAR", "COMMA", "ASSIGN", "STAR", 
  "PLUS", "MINUS", "TILDE", "PIPE2", "DIV", "MOD", "LT2", "GT2", "AMP", 
  "PIPE", "LT", "LT_EQ", "GT", "GT_EQ", "EQ", "NOT_EQ1", "NOT_EQ2", "K_ABORT", 
  "K_ACTION", "K_ADD", "K_AFTER", "K_ALL", "K_ALTER", "K_ANALYZE", "K_AND", 
  "K_AS", "K_ASC", "K_ATTACH", "K_AUTOINCREMENT", "K_BEFORE", "K_BEGIN", 
  "K_BETWEEN", "K_BY", "K_CASCADE", "K_CASE", "K_CAST", "K_CHECK", "K_COLLATE", 
  "K_COLUMN", "K_COMMIT", "K_CONFLICT", "K_CONSTRAINT", "K_CREATE", "K_CROSS", 
  "K_CURRENT_DATE", "K_CURRENT_TIME", "K_CURRENT_TIMESTAMP", "K_DATABASE", 
  "K_DEFAULT", "K_DEFERRABLE", "K_DEFERRED", "K_DELETE", "K_DESC", "K_DETACH", 
  "K_DISTINCT", "K_DROP", "K_EACH", "K_ELSE", "K_END", "K_ESCAPE", "K_EXCEPT", 
  "K_EXCLUSIVE", "K_EXISTS", "K_EXPLAIN", "K_FAIL", "K_FOR", "K_FOREIGN", 
  "K_FROM", "K_FULL", "K_GLOB", "K_GROUP", "K_HAVING", "K_IF", "K_IGNORE", 
  "K_IMMEDIATE", "K_IN", "K_INDEX", "K_INDEXED", "K_INITIALLY", "K_INNER", 
  "K_INSERT", "K_INSTEAD", "K_INTERSECT", "K_INTO", "K_IS", "K_ISNULL", 
  "K_JOIN", "K_KEY", "K_LEFT", "K_LIKE", "K_LIMIT", "K_MATCH", "K_NATURAL", 
  "K_NO", "K_NOT", "K_NOTNULL", "K_NULL", "K_OF", "K_OFFSET", "K_ON", "K_OR", 
  "K_ORDER", "K_OUTER", "K_PLAN", "K_PRAGMA", "K_PRIMARY", "K_QUERY", "K_RAISE", 
  "K_RECURSIVE", "K_REFERENCES", "K_REGEXP", "K_REINDEX", "K_RELEASE", "K_RENAME", 
  "K_REPLACE", "K_RESTRICT", "K_RIGHT", "K_ROLLBACK", "K_ROW", "K_SAVEPOINT", 
  "K_SELECT", "K_SET", "K_TABLE", "K_TEMP", "K_TEMPORARY", "K_THEN", "K_TO", 
  "K_TRANSACTION", "K_TRIGGER", "K_UNION", "K_UNIQUE", "K_UPDATE", "K_USING", 
  "K_VACUUM", "K_VALUES", "K_VIEW", "K_VIRTUAL", "K_WHEN", "K_WHERE", "K_WITH", 
  "K_WITHOUT", "IDENTIFIER", "NUMERIC_LITERAL", "BIND_PARAMETER", "STRING_LITERAL", 
  "BLOB_LITERAL", "SINGLE_LINE_COMMENT", "MULTILINE_COMMENT", "SPACES", 
  "UNEXPECTED_CHAR"
};

dfa::Vocabulary NormalSQLParser::_vocabulary(_literalNames, _symbolicNames);

std::vector<std::string> NormalSQLParser::_tokenNames;

NormalSQLParser::Initializer::Initializer() {
	for (size_t i = 0; i < _symbolicNames.size(); ++i) {
		std::string name = _vocabulary.getLiteralName(i);
		if (name.empty()) {
			name = _vocabulary.getSymbolicName(i);
		}

		if (name.empty()) {
			_tokenNames.push_back("<INVALID>");
		} else {
      _tokenNames.push_back(name);
    }
	}

  _serializedATN = {
    0x3, 0x608b, 0xa72a, 0x8133, 0xb9ed, 0x417c, 0x3be7, 0x7786, 0x5964, 
    0x3, 0xa0, 0x2db, 0x4, 0x2, 0x9, 0x2, 0x4, 0x3, 0x9, 0x3, 0x4, 0x4, 
    0x9, 0x4, 0x4, 0x5, 0x9, 0x5, 0x4, 0x6, 0x9, 0x6, 0x4, 0x7, 0x9, 0x7, 
    0x4, 0x8, 0x9, 0x8, 0x4, 0x9, 0x9, 0x9, 0x4, 0xa, 0x9, 0xa, 0x4, 0xb, 
    0x9, 0xb, 0x4, 0xc, 0x9, 0xc, 0x4, 0xd, 0x9, 0xd, 0x4, 0xe, 0x9, 0xe, 
    0x4, 0xf, 0x9, 0xf, 0x4, 0x10, 0x9, 0x10, 0x4, 0x11, 0x9, 0x11, 0x4, 
    0x12, 0x9, 0x12, 0x4, 0x13, 0x9, 0x13, 0x4, 0x14, 0x9, 0x14, 0x4, 0x15, 
    0x9, 0x15, 0x4, 0x16, 0x9, 0x16, 0x4, 0x17, 0x9, 0x17, 0x4, 0x18, 0x9, 
    0x18, 0x4, 0x19, 0x9, 0x19, 0x4, 0x1a, 0x9, 0x1a, 0x4, 0x1b, 0x9, 0x1b, 
    0x4, 0x1c, 0x9, 0x1c, 0x4, 0x1d, 0x9, 0x1d, 0x4, 0x1e, 0x9, 0x1e, 0x4, 
    0x1f, 0x9, 0x1f, 0x4, 0x20, 0x9, 0x20, 0x4, 0x21, 0x9, 0x21, 0x4, 0x22, 
    0x9, 0x22, 0x4, 0x23, 0x9, 0x23, 0x4, 0x24, 0x9, 0x24, 0x4, 0x25, 0x9, 
    0x25, 0x4, 0x26, 0x9, 0x26, 0x4, 0x27, 0x9, 0x27, 0x4, 0x28, 0x9, 0x28, 
    0x4, 0x29, 0x9, 0x29, 0x4, 0x2a, 0x9, 0x2a, 0x4, 0x2b, 0x9, 0x2b, 0x4, 
    0x2c, 0x9, 0x2c, 0x4, 0x2d, 0x9, 0x2d, 0x4, 0x2e, 0x9, 0x2e, 0x4, 0x2f, 
    0x9, 0x2f, 0x4, 0x30, 0x9, 0x30, 0x4, 0x31, 0x9, 0x31, 0x4, 0x32, 0x9, 
    0x32, 0x3, 0x2, 0x3, 0x2, 0x7, 0x2, 0x67, 0xa, 0x2, 0xc, 0x2, 0xe, 0x2, 
    0x6a, 0xb, 0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 
    0x4, 0x7, 0x4, 0x72, 0xa, 0x4, 0xc, 0x4, 0xe, 0x4, 0x75, 0xb, 0x4, 0x3, 
    0x4, 0x3, 0x4, 0x6, 0x4, 0x79, 0xa, 0x4, 0xd, 0x4, 0xe, 0x4, 0x7a, 0x3, 
    0x4, 0x7, 0x4, 0x7e, 0xa, 0x4, 0xc, 0x4, 0xe, 0x4, 0x81, 0xb, 0x4, 0x3, 
    0x4, 0x7, 0x4, 0x84, 0xa, 0x4, 0xc, 0x4, 0xe, 0x4, 0x87, 0xb, 0x4, 0x3, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x8c, 0xa, 0x5, 0x5, 0x5, 0x8e, 0xa, 
    0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x6, 0x3, 0x6, 0x5, 0x6, 0x94, 0xa, 0x6, 
    0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x7, 0x6, 0x99, 0xa, 0x6, 0xc, 0x6, 0xe, 
    0x6, 0x9c, 0xb, 0x6, 0x5, 0x6, 0x9e, 0xa, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 
    0x6, 0x3, 0x6, 0x7, 0x6, 0xa4, 0xa, 0x6, 0xc, 0x6, 0xe, 0x6, 0xa7, 0xb, 
    0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x7, 0x6, 0xae, 
    0xa, 0x6, 0xc, 0x6, 0xe, 0x6, 0xb1, 0xb, 0x6, 0x5, 0x6, 0xb3, 0xa, 0x6, 
    0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x5, 0x6, 0xb9, 0xa, 0x6, 0x5, 
    0x6, 0xbb, 0xa, 0x6, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0xbf, 0xa, 0x7, 0x3, 
    0x7, 0x3, 0x7, 0x3, 0x7, 0x7, 0x7, 0xc4, 0xa, 0x7, 0xc, 0x7, 0xe, 0x7, 
    0xc7, 0xb, 0x7, 0x3, 0x7, 0x5, 0x7, 0xca, 0xa, 0x7, 0x3, 0x7, 0x5, 0x7, 
    0xcd, 0xa, 0x7, 0x3, 0x7, 0x5, 0x7, 0xd0, 0xa, 0x7, 0x3, 0x7, 0x3, 0x7, 
    0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x7, 0x7, 0xd7, 0xa, 0x7, 0xc, 0x7, 0xe, 
    0x7, 0xda, 0xb, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 
    0x3, 0x7, 0x7, 0x7, 0xe2, 0xa, 0x7, 0xc, 0x7, 0xe, 0x7, 0xe5, 0xb, 0x7, 
    0x3, 0x7, 0x3, 0x7, 0x7, 0x7, 0xe9, 0xa, 0x7, 0xc, 0x7, 0xe, 0x7, 0xec, 
    0xb, 0x7, 0x5, 0x7, 0xee, 0xa, 0x7, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 
    0x8, 0x7, 0x8, 0xf4, 0xa, 0x8, 0xc, 0x8, 0xe, 0x8, 0xf7, 0xb, 0x8, 0x3, 
    0x8, 0x5, 0x8, 0xfa, 0xa, 0x8, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0xa, 
    0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x7, 0xa, 0x104, 0xa, 0xa, 0xc, 
    0xa, 0xe, 0xa, 0x107, 0xb, 0xa, 0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 0x10b, 
    0xa, 0xa, 0x3, 0xb, 0x6, 0xb, 0x10e, 0xa, 0xb, 0xd, 0xb, 0xe, 0xb, 0x10f, 
    0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 
    0x3, 0xb, 0x3, 0xb, 0x3, 0xb, 0x5, 0xb, 0x11c, 0xa, 0xb, 0x3, 0xc, 0x3, 
    0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x124, 0xa, 0xc, 
    0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x129, 0xa, 0xc, 0x3, 0xc, 0x3, 
    0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x132, 
    0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x7, 0xc, 0x137, 0xa, 0xc, 0xc, 
    0xc, 0xe, 0xc, 0x13a, 0xb, 0xc, 0x3, 0xc, 0x5, 0xc, 0x13d, 0xa, 0xc, 
    0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 
    0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 
    0x5, 0xc, 0x14d, 0xa, 0xc, 0x3, 0xc, 0x5, 0xc, 0x150, 0xa, 0xc, 0x3, 
    0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x158, 
    0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x6, 0xc, 
    0x15f, 0xa, 0xc, 0xd, 0xc, 0xe, 0xc, 0x160, 0x3, 0xc, 0x3, 0xc, 0x5, 
    0xc, 0x165, 0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x16a, 
    0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 
    0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 
    0x3, 0xc, 0x5, 0xc, 0x17a, 0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 
    0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 
    0xc, 0x5, 0xc, 0x187, 0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 
    0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 
    0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 
    0x5, 0xc, 0x19b, 0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 
    0xc, 0x1a1, 0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 
    0x5, 0xc, 0x1a8, 0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x1ac, 0xa, 
    0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x7, 
    0xc, 0x1b4, 0xa, 0xc, 0xc, 0xc, 0xe, 0xc, 0x1b7, 0xb, 0xc, 0x5, 0xc, 
    0x1b9, 0xa, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x5, 0xc, 0x1bf, 
    0xa, 0xc, 0x3, 0xc, 0x5, 0xc, 0x1c2, 0xa, 0xc, 0x7, 0xc, 0x1c4, 0xa, 
    0xc, 0xc, 0xc, 0xe, 0xc, 0x1c7, 0xb, 0xc, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 
    0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x5, 0xd, 0x1cf, 0xa, 0xd, 0x3, 0xd, 0x3, 
    0xd, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x5, 0xe, 0x1d6, 0xa, 0xe, 0x3, 0xe, 
    0x5, 0xe, 0x1d9, 0xa, 0xe, 0x3, 0xf, 0x3, 0xf, 0x5, 0xf, 0x1dd, 0xa, 
    0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 
    0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x7, 0xf, 0x1eb, 
    0xa, 0xf, 0xc, 0xf, 0xe, 0xf, 0x1ee, 0xb, 0xf, 0x3, 0x10, 0x3, 0x10, 
    0x3, 0x10, 0x5, 0x10, 0x1f3, 0xa, 0x10, 0x3, 0x10, 0x3, 0x10, 0x3, 0x10, 
    0x3, 0x10, 0x3, 0x10, 0x3, 0x10, 0x5, 0x10, 0x1fb, 0xa, 0x10, 0x3, 0x11, 
    0x3, 0x11, 0x3, 0x11, 0x5, 0x11, 0x200, 0xa, 0x11, 0x3, 0x11, 0x5, 0x11, 
    0x203, 0xa, 0x11, 0x3, 0x12, 0x3, 0x12, 0x3, 0x12, 0x5, 0x12, 0x208, 
    0xa, 0x12, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x7, 
    0x13, 0x20f, 0xa, 0x13, 0xc, 0x13, 0xe, 0x13, 0x212, 0xb, 0x13, 0x3, 
    0x13, 0x3, 0x13, 0x5, 0x13, 0x216, 0xa, 0x13, 0x3, 0x13, 0x3, 0x13, 
    0x3, 0x13, 0x3, 0x13, 0x3, 0x13, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x3, 
    0x14, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x5, 0x14, 0x224, 0xa, 0x14, 
    0x3, 0x14, 0x5, 0x14, 0x227, 0xa, 0x14, 0x5, 0x14, 0x229, 0xa, 0x14, 
    0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x5, 0x15, 0x22e, 0xa, 0x15, 0x3, 0x15, 
    0x3, 0x15, 0x5, 0x15, 0x232, 0xa, 0x15, 0x3, 0x15, 0x5, 0x15, 0x235, 
    0xa, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x5, 
    0x15, 0x23c, 0xa, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 
    0x7, 0x15, 0x242, 0xa, 0x15, 0xc, 0x15, 0xe, 0x15, 0x245, 0xb, 0x15, 
    0x3, 0x15, 0x5, 0x15, 0x248, 0xa, 0x15, 0x3, 0x15, 0x3, 0x15, 0x5, 0x15, 
    0x24c, 0xa, 0x15, 0x3, 0x15, 0x5, 0x15, 0x24f, 0xa, 0x15, 0x3, 0x15, 
    0x3, 0x15, 0x3, 0x15, 0x3, 0x15, 0x5, 0x15, 0x255, 0xa, 0x15, 0x3, 0x15, 
    0x5, 0x15, 0x258, 0xa, 0x15, 0x5, 0x15, 0x25a, 0xa, 0x15, 0x3, 0x16, 
    0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 0x7, 0x16, 0x261, 0xa, 0x16, 
    0xc, 0x16, 0xe, 0x16, 0x264, 0xb, 0x16, 0x3, 0x17, 0x3, 0x17, 0x5, 0x17, 
    0x268, 0xa, 0x17, 0x3, 0x17, 0x3, 0x17, 0x5, 0x17, 0x26c, 0xa, 0x17, 
    0x3, 0x17, 0x3, 0x17, 0x5, 0x17, 0x270, 0xa, 0x17, 0x3, 0x17, 0x5, 0x17, 
    0x273, 0xa, 0x17, 0x3, 0x18, 0x3, 0x18, 0x3, 0x18, 0x3, 0x18, 0x3, 0x18, 
    0x3, 0x18, 0x3, 0x18, 0x7, 0x18, 0x27c, 0xa, 0x18, 0xc, 0x18, 0xe, 0x18, 
    0x27f, 0xb, 0x18, 0x3, 0x18, 0x3, 0x18, 0x5, 0x18, 0x283, 0xa, 0x18, 
    0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 0x3, 0x19, 0x5, 0x19, 0x28a, 
    0xa, 0x19, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1a, 0x7, 
    0x1a, 0x291, 0xa, 0x1a, 0xc, 0x1a, 0xe, 0x1a, 0x294, 0xb, 0x1a, 0x3, 
    0x1a, 0x3, 0x1a, 0x5, 0x1a, 0x298, 0xa, 0x1a, 0x3, 0x1b, 0x5, 0x1b, 
    0x29b, 0xa, 0x1b, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 
    0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x3, 0x1c, 0x5, 0x1c, 0x2a6, 0xa, 0x1c, 
    0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1e, 0x3, 0x1e, 0x3, 0x1f, 0x3, 0x1f, 0x3, 
    0x20, 0x3, 0x20, 0x3, 0x21, 0x3, 0x21, 0x3, 0x22, 0x3, 0x22, 0x3, 0x23, 
    0x3, 0x23, 0x3, 0x24, 0x3, 0x24, 0x3, 0x25, 0x3, 0x25, 0x3, 0x26, 0x3, 
    0x26, 0x3, 0x27, 0x3, 0x27, 0x3, 0x28, 0x3, 0x28, 0x3, 0x29, 0x3, 0x29, 
    0x3, 0x2a, 0x3, 0x2a, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2c, 0x3, 0x2c, 0x3, 
    0x2d, 0x3, 0x2d, 0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2f, 0x3, 0x2f, 0x3, 0x30, 
    0x3, 0x30, 0x3, 0x31, 0x3, 0x31, 0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x3, 
    0x32, 0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x5, 0x32, 0x2d9, 0xa, 0x32, 
    0x3, 0x32, 0x2, 0x3, 0x16, 0x33, 0x2, 0x4, 0x6, 0x8, 0xa, 0xc, 0xe, 
    0x10, 0x12, 0x14, 0x16, 0x18, 0x1a, 0x1c, 0x1e, 0x20, 0x22, 0x24, 0x26, 
    0x28, 0x2a, 0x2c, 0x2e, 0x30, 0x32, 0x34, 0x36, 0x38, 0x3a, 0x3c, 0x3e, 
    0x40, 0x42, 0x44, 0x46, 0x48, 0x4a, 0x4c, 0x4e, 0x50, 0x52, 0x54, 0x56, 
    0x58, 0x5a, 0x5c, 0x5e, 0x60, 0x62, 0x2, 0xf, 0x4, 0x2, 0x8, 0x8, 0x6d, 
    0x6d, 0x4, 0x2, 0x20, 0x20, 0x41, 0x41, 0x4, 0x2, 0xa, 0xa, 0xf, 0x10, 
    0x3, 0x2, 0xb, 0xc, 0x3, 0x2, 0x11, 0x14, 0x3, 0x2, 0x15, 0x18, 0x4, 
    0x2, 0x9, 0x9, 0x19, 0x1b, 0x6, 0x2, 0x50, 0x50, 0x64, 0x64, 0x66, 0x66, 
    0x79, 0x79, 0x5, 0x2, 0x1c, 0x1c, 0x4b, 0x4b, 0x80, 0x80, 0x4, 0x2, 
    0x25, 0x25, 0x3f, 0x3f, 0x4, 0x2, 0xb, 0xd, 0x69, 0x69, 0x4, 0x2, 0x98, 
    0x98, 0x9b, 0x9b, 0x3, 0x2, 0x1c, 0x97, 0x2, 0x330, 0x2, 0x68, 0x3, 
    0x2, 0x2, 0x2, 0x4, 0x6d, 0x3, 0x2, 0x2, 0x2, 0x6, 0x73, 0x3, 0x2, 0x2, 
    0x2, 0x8, 0x8d, 0x3, 0x2, 0x2, 0x2, 0xa, 0x9d, 0x3, 0x2, 0x2, 0x2, 0xc, 
    0xed, 0x3, 0x2, 0x2, 0x2, 0xe, 0xef, 0x3, 0x2, 0x2, 0x2, 0x10, 0xfb, 
    0x3, 0x2, 0x2, 0x2, 0x12, 0xfe, 0x3, 0x2, 0x2, 0x2, 0x14, 0x10d, 0x3, 
    0x2, 0x2, 0x2, 0x16, 0x169, 0x3, 0x2, 0x2, 0x2, 0x18, 0x1c8, 0x3, 0x2, 
    0x2, 0x2, 0x1a, 0x1d2, 0x3, 0x2, 0x2, 0x2, 0x1c, 0x1da, 0x3, 0x2, 0x2, 
    0x2, 0x1e, 0x1f2, 0x3, 0x2, 0x2, 0x2, 0x20, 0x1fc, 0x3, 0x2, 0x2, 0x2, 
    0x22, 0x207, 0x3, 0x2, 0x2, 0x2, 0x24, 0x209, 0x3, 0x2, 0x2, 0x2, 0x26, 
    0x228, 0x3, 0x2, 0x2, 0x2, 0x28, 0x259, 0x3, 0x2, 0x2, 0x2, 0x2a, 0x25b, 
    0x3, 0x2, 0x2, 0x2, 0x2c, 0x272, 0x3, 0x2, 0x2, 0x2, 0x2e, 0x282, 0x3, 
    0x2, 0x2, 0x2, 0x30, 0x289, 0x3, 0x2, 0x2, 0x2, 0x32, 0x28b, 0x3, 0x2, 
    0x2, 0x2, 0x34, 0x29a, 0x3, 0x2, 0x2, 0x2, 0x36, 0x2a5, 0x3, 0x2, 0x2, 
    0x2, 0x38, 0x2a7, 0x3, 0x2, 0x2, 0x2, 0x3a, 0x2a9, 0x3, 0x2, 0x2, 0x2, 
    0x3c, 0x2ab, 0x3, 0x2, 0x2, 0x2, 0x3e, 0x2ad, 0x3, 0x2, 0x2, 0x2, 0x40, 
    0x2af, 0x3, 0x2, 0x2, 0x2, 0x42, 0x2b1, 0x3, 0x2, 0x2, 0x2, 0x44, 0x2b3, 
    0x3, 0x2, 0x2, 0x2, 0x46, 0x2b5, 0x3, 0x2, 0x2, 0x2, 0x48, 0x2b7, 0x3, 
    0x2, 0x2, 0x2, 0x4a, 0x2b9, 0x3, 0x2, 0x2, 0x2, 0x4c, 0x2bb, 0x3, 0x2, 
    0x2, 0x2, 0x4e, 0x2bd, 0x3, 0x2, 0x2, 0x2, 0x50, 0x2bf, 0x3, 0x2, 0x2, 
    0x2, 0x52, 0x2c1, 0x3, 0x2, 0x2, 0x2, 0x54, 0x2c3, 0x3, 0x2, 0x2, 0x2, 
    0x56, 0x2c5, 0x3, 0x2, 0x2, 0x2, 0x58, 0x2c7, 0x3, 0x2, 0x2, 0x2, 0x5a, 
    0x2c9, 0x3, 0x2, 0x2, 0x2, 0x5c, 0x2cb, 0x3, 0x2, 0x2, 0x2, 0x5e, 0x2cd, 
    0x3, 0x2, 0x2, 0x2, 0x60, 0x2cf, 0x3, 0x2, 0x2, 0x2, 0x62, 0x2d8, 0x3, 
    0x2, 0x2, 0x2, 0x64, 0x67, 0x5, 0x6, 0x4, 0x2, 0x65, 0x67, 0x5, 0x4, 
    0x3, 0x2, 0x66, 0x64, 0x3, 0x2, 0x2, 0x2, 0x66, 0x65, 0x3, 0x2, 0x2, 
    0x2, 0x67, 0x6a, 0x3, 0x2, 0x2, 0x2, 0x68, 0x66, 0x3, 0x2, 0x2, 0x2, 
    0x68, 0x69, 0x3, 0x2, 0x2, 0x2, 0x69, 0x6b, 0x3, 0x2, 0x2, 0x2, 0x6a, 
    0x68, 0x3, 0x2, 0x2, 0x2, 0x6b, 0x6c, 0x7, 0x2, 0x2, 0x3, 0x6c, 0x3, 
    0x3, 0x2, 0x2, 0x2, 0x6d, 0x6e, 0x7, 0xa0, 0x2, 0x2, 0x6e, 0x6f, 0x8, 
    0x3, 0x1, 0x2, 0x6f, 0x5, 0x3, 0x2, 0x2, 0x2, 0x70, 0x72, 0x7, 0x4, 
    0x2, 0x2, 0x71, 0x70, 0x3, 0x2, 0x2, 0x2, 0x72, 0x75, 0x3, 0x2, 0x2, 
    0x2, 0x73, 0x71, 0x3, 0x2, 0x2, 0x2, 0x73, 0x74, 0x3, 0x2, 0x2, 0x2, 
    0x74, 0x76, 0x3, 0x2, 0x2, 0x2, 0x75, 0x73, 0x3, 0x2, 0x2, 0x2, 0x76, 
    0x7f, 0x5, 0x8, 0x5, 0x2, 0x77, 0x79, 0x7, 0x4, 0x2, 0x2, 0x78, 0x77, 
    0x3, 0x2, 0x2, 0x2, 0x79, 0x7a, 0x3, 0x2, 0x2, 0x2, 0x7a, 0x78, 0x3, 
    0x2, 0x2, 0x2, 0x7a, 0x7b, 0x3, 0x2, 0x2, 0x2, 0x7b, 0x7c, 0x3, 0x2, 
    0x2, 0x2, 0x7c, 0x7e, 0x5, 0x8, 0x5, 0x2, 0x7d, 0x78, 0x3, 0x2, 0x2, 
    0x2, 0x7e, 0x81, 0x3, 0x2, 0x2, 0x2, 0x7f, 0x7d, 0x3, 0x2, 0x2, 0x2, 
    0x7f, 0x80, 0x3, 0x2, 0x2, 0x2, 0x80, 0x85, 0x3, 0x2, 0x2, 0x2, 0x81, 
    0x7f, 0x3, 0x2, 0x2, 0x2, 0x82, 0x84, 0x7, 0x4, 0x2, 0x2, 0x83, 0x82, 
    0x3, 0x2, 0x2, 0x2, 0x84, 0x87, 0x3, 0x2, 0x2, 0x2, 0x85, 0x83, 0x3, 
    0x2, 0x2, 0x2, 0x85, 0x86, 0x3, 0x2, 0x2, 0x2, 0x86, 0x7, 0x3, 0x2, 
    0x2, 0x2, 0x87, 0x85, 0x3, 0x2, 0x2, 0x2, 0x88, 0x8b, 0x7, 0x4a, 0x2, 
    0x2, 0x89, 0x8a, 0x7, 0x75, 0x2, 0x2, 0x8a, 0x8c, 0x7, 0x72, 0x2, 0x2, 
    0x8b, 0x89, 0x3, 0x2, 0x2, 0x2, 0x8b, 0x8c, 0x3, 0x2, 0x2, 0x2, 0x8c, 
    0x8e, 0x3, 0x2, 0x2, 0x2, 0x8d, 0x88, 0x3, 0x2, 0x2, 0x2, 0x8d, 0x8e, 
    0x3, 0x2, 0x2, 0x2, 0x8e, 0x8f, 0x3, 0x2, 0x2, 0x2, 0x8f, 0x90, 0x5, 
    0xa, 0x6, 0x2, 0x90, 0x9, 0x3, 0x2, 0x2, 0x2, 0x91, 0x93, 0x7, 0x96, 
    0x2, 0x2, 0x92, 0x94, 0x7, 0x77, 0x2, 0x2, 0x93, 0x92, 0x3, 0x2, 0x2, 
    0x2, 0x93, 0x94, 0x3, 0x2, 0x2, 0x2, 0x94, 0x95, 0x3, 0x2, 0x2, 0x2, 
    0x95, 0x9a, 0x5, 0x24, 0x13, 0x2, 0x96, 0x97, 0x7, 0x8, 0x2, 0x2, 0x97, 
    0x99, 0x5, 0x24, 0x13, 0x2, 0x98, 0x96, 0x3, 0x2, 0x2, 0x2, 0x99, 0x9c, 
    0x3, 0x2, 0x2, 0x2, 0x9a, 0x98, 0x3, 0x2, 0x2, 0x2, 0x9a, 0x9b, 0x3, 
    0x2, 0x2, 0x2, 0x9b, 0x9e, 0x3, 0x2, 0x2, 0x2, 0x9c, 0x9a, 0x3, 0x2, 
    0x2, 0x2, 0x9d, 0x91, 0x3, 0x2, 0x2, 0x2, 0x9d, 0x9e, 0x3, 0x2, 0x2, 
    0x2, 0x9e, 0x9f, 0x3, 0x2, 0x2, 0x2, 0x9f, 0xa5, 0x5, 0xc, 0x7, 0x2, 
    0xa0, 0xa1, 0x5, 0x30, 0x19, 0x2, 0xa1, 0xa2, 0x5, 0xc, 0x7, 0x2, 0xa2, 
    0xa4, 0x3, 0x2, 0x2, 0x2, 0xa3, 0xa0, 0x3, 0x2, 0x2, 0x2, 0xa4, 0xa7, 
    0x3, 0x2, 0x2, 0x2, 0xa5, 0xa3, 0x3, 0x2, 0x2, 0x2, 0xa5, 0xa6, 0x3, 
    0x2, 0x2, 0x2, 0xa6, 0xb2, 0x3, 0x2, 0x2, 0x2, 0xa7, 0xa5, 0x3, 0x2, 
    0x2, 0x2, 0xa8, 0xa9, 0x7, 0x70, 0x2, 0x2, 0xa9, 0xaa, 0x7, 0x2b, 0x2, 
    0x2, 0xaa, 0xaf, 0x5, 0x20, 0x11, 0x2, 0xab, 0xac, 0x7, 0x8, 0x2, 0x2, 
    0xac, 0xae, 0x5, 0x20, 0x11, 0x2, 0xad, 0xab, 0x3, 0x2, 0x2, 0x2, 0xae, 
    0xb1, 0x3, 0x2, 0x2, 0x2, 0xaf, 0xad, 0x3, 0x2, 0x2, 0x2, 0xaf, 0xb0, 
    0x3, 0x2, 0x2, 0x2, 0xb0, 0xb3, 0x3, 0x2, 0x2, 0x2, 0xb1, 0xaf, 0x3, 
    0x2, 0x2, 0x2, 0xb2, 0xa8, 0x3, 0x2, 0x2, 0x2, 0xb2, 0xb3, 0x3, 0x2, 
    0x2, 0x2, 0xb3, 0xba, 0x3, 0x2, 0x2, 0x2, 0xb4, 0xb5, 0x7, 0x65, 0x2, 
    0x2, 0xb5, 0xb8, 0x5, 0x16, 0xc, 0x2, 0xb6, 0xb7, 0x9, 0x2, 0x2, 0x2, 
    0xb7, 0xb9, 0x5, 0x16, 0xc, 0x2, 0xb8, 0xb6, 0x3, 0x2, 0x2, 0x2, 0xb8, 
    0xb9, 0x3, 0x2, 0x2, 0x2, 0xb9, 0xbb, 0x3, 0x2, 0x2, 0x2, 0xba, 0xb4, 
    0x3, 0x2, 0x2, 0x2, 0xba, 0xbb, 0x3, 0x2, 0x2, 0x2, 0xbb, 0xb, 0x3, 
    0x2, 0x2, 0x2, 0xbc, 0xbe, 0x7, 0x83, 0x2, 0x2, 0xbd, 0xbf, 0x9, 0x3, 
    0x2, 0x2, 0xbe, 0xbd, 0x3, 0x2, 0x2, 0x2, 0xbe, 0xbf, 0x3, 0x2, 0x2, 
    0x2, 0xbf, 0xc0, 0x3, 0x2, 0x2, 0x2, 0xc0, 0xc5, 0x5, 0x26, 0x14, 0x2, 
    0xc1, 0xc2, 0x7, 0x8, 0x2, 0x2, 0xc2, 0xc4, 0x5, 0x26, 0x14, 0x2, 0xc3, 
    0xc1, 0x3, 0x2, 0x2, 0x2, 0xc4, 0xc7, 0x3, 0x2, 0x2, 0x2, 0xc5, 0xc3, 
    0x3, 0x2, 0x2, 0x2, 0xc5, 0xc6, 0x3, 0x2, 0x2, 0x2, 0xc6, 0xc9, 0x3, 
    0x2, 0x2, 0x2, 0xc7, 0xc5, 0x3, 0x2, 0x2, 0x2, 0xc8, 0xca, 0x5, 0xe, 
    0x8, 0x2, 0xc9, 0xc8, 0x3, 0x2, 0x2, 0x2, 0xc9, 0xca, 0x3, 0x2, 0x2, 
    0x2, 0xca, 0xcc, 0x3, 0x2, 0x2, 0x2, 0xcb, 0xcd, 0x5, 0x10, 0x9, 0x2, 
    0xcc, 0xcb, 0x3, 0x2, 0x2, 0x2, 0xcc, 0xcd, 0x3, 0x2, 0x2, 0x2, 0xcd, 
    0xcf, 0x3, 0x2, 0x2, 0x2, 0xce, 0xd0, 0x5, 0x12, 0xa, 0x2, 0xcf, 0xce, 
    0x3, 0x2, 0x2, 0x2, 0xcf, 0xd0, 0x3, 0x2, 0x2, 0x2, 0xd0, 0xee, 0x3, 
    0x2, 0x2, 0x2, 0xd1, 0xd2, 0x7, 0x91, 0x2, 0x2, 0xd2, 0xd3, 0x7, 0x6, 
    0x2, 0x2, 0xd3, 0xd8, 0x5, 0x16, 0xc, 0x2, 0xd4, 0xd5, 0x7, 0x8, 0x2, 
    0x2, 0xd5, 0xd7, 0x5, 0x16, 0xc, 0x2, 0xd6, 0xd4, 0x3, 0x2, 0x2, 0x2, 
    0xd7, 0xda, 0x3, 0x2, 0x2, 0x2, 0xd8, 0xd6, 0x3, 0x2, 0x2, 0x2, 0xd8, 
    0xd9, 0x3, 0x2, 0x2, 0x2, 0xd9, 0xdb, 0x3, 0x2, 0x2, 0x2, 0xda, 0xd8, 
    0x3, 0x2, 0x2, 0x2, 0xdb, 0xea, 0x7, 0x7, 0x2, 0x2, 0xdc, 0xdd, 0x7, 
    0x8, 0x2, 0x2, 0xdd, 0xde, 0x7, 0x6, 0x2, 0x2, 0xde, 0xe3, 0x5, 0x16, 
    0xc, 0x2, 0xdf, 0xe0, 0x7, 0x8, 0x2, 0x2, 0xe0, 0xe2, 0x5, 0x16, 0xc, 
    0x2, 0xe1, 0xdf, 0x3, 0x2, 0x2, 0x2, 0xe2, 0xe5, 0x3, 0x2, 0x2, 0x2, 
    0xe3, 0xe1, 0x3, 0x2, 0x2, 0x2, 0xe3, 0xe4, 0x3, 0x2, 0x2, 0x2, 0xe4, 
    0xe6, 0x3, 0x2, 0x2, 0x2, 0xe5, 0xe3, 0x3, 0x2, 0x2, 0x2, 0xe6, 0xe7, 
    0x7, 0x7, 0x2, 0x2, 0xe7, 0xe9, 0x3, 0x2, 0x2, 0x2, 0xe8, 0xdc, 0x3, 
    0x2, 0x2, 0x2, 0xe9, 0xec, 0x3, 0x2, 0x2, 0x2, 0xea, 0xe8, 0x3, 0x2, 
    0x2, 0x2, 0xea, 0xeb, 0x3, 0x2, 0x2, 0x2, 0xeb, 0xee, 0x3, 0x2, 0x2, 
    0x2, 0xec, 0xea, 0x3, 0x2, 0x2, 0x2, 0xed, 0xbc, 0x3, 0x2, 0x2, 0x2, 
    0xed, 0xd1, 0x3, 0x2, 0x2, 0x2, 0xee, 0xd, 0x3, 0x2, 0x2, 0x2, 0xef, 
    0xf9, 0x7, 0x4e, 0x2, 0x2, 0xf0, 0xf5, 0x5, 0x28, 0x15, 0x2, 0xf1, 0xf2, 
    0x7, 0x8, 0x2, 0x2, 0xf2, 0xf4, 0x5, 0x28, 0x15, 0x2, 0xf3, 0xf1, 0x3, 
    0x2, 0x2, 0x2, 0xf4, 0xf7, 0x3, 0x2, 0x2, 0x2, 0xf5, 0xf3, 0x3, 0x2, 
    0x2, 0x2, 0xf5, 0xf6, 0x3, 0x2, 0x2, 0x2, 0xf6, 0xfa, 0x3, 0x2, 0x2, 
    0x2, 0xf7, 0xf5, 0x3, 0x2, 0x2, 0x2, 0xf8, 0xfa, 0x5, 0x2a, 0x16, 0x2, 
    0xf9, 0xf0, 0x3, 0x2, 0x2, 0x2, 0xf9, 0xf8, 0x3, 0x2, 0x2, 0x2, 0xfa, 
    0xf, 0x3, 0x2, 0x2, 0x2, 0xfb, 0xfc, 0x7, 0x95, 0x2, 0x2, 0xfc, 0xfd, 
    0x5, 0x16, 0xc, 0x2, 0xfd, 0x11, 0x3, 0x2, 0x2, 0x2, 0xfe, 0xff, 0x7, 
    0x51, 0x2, 0x2, 0xff, 0x100, 0x7, 0x2b, 0x2, 0x2, 0x100, 0x105, 0x5, 
    0x16, 0xc, 0x2, 0x101, 0x102, 0x7, 0x8, 0x2, 0x2, 0x102, 0x104, 0x5, 
    0x16, 0xc, 0x2, 0x103, 0x101, 0x3, 0x2, 0x2, 0x2, 0x104, 0x107, 0x3, 
    0x2, 0x2, 0x2, 0x105, 0x103, 0x3, 0x2, 0x2, 0x2, 0x105, 0x106, 0x3, 
    0x2, 0x2, 0x2, 0x106, 0x10a, 0x3, 0x2, 0x2, 0x2, 0x107, 0x105, 0x3, 
    0x2, 0x2, 0x2, 0x108, 0x109, 0x7, 0x52, 0x2, 0x2, 0x109, 0x10b, 0x5, 
    0x16, 0xc, 0x2, 0x10a, 0x108, 0x3, 0x2, 0x2, 0x2, 0x10a, 0x10b, 0x3, 
    0x2, 0x2, 0x2, 0x10b, 0x13, 0x3, 0x2, 0x2, 0x2, 0x10c, 0x10e, 0x5, 0x40, 
    0x21, 0x2, 0x10d, 0x10c, 0x3, 0x2, 0x2, 0x2, 0x10e, 0x10f, 0x3, 0x2, 
    0x2, 0x2, 0x10f, 0x10d, 0x3, 0x2, 0x2, 0x2, 0x10f, 0x110, 0x3, 0x2, 
    0x2, 0x2, 0x110, 0x11b, 0x3, 0x2, 0x2, 0x2, 0x111, 0x112, 0x7, 0x6, 
    0x2, 0x2, 0x112, 0x113, 0x5, 0x34, 0x1b, 0x2, 0x113, 0x114, 0x7, 0x7, 
    0x2, 0x2, 0x114, 0x11c, 0x3, 0x2, 0x2, 0x2, 0x115, 0x116, 0x7, 0x6, 
    0x2, 0x2, 0x116, 0x117, 0x5, 0x34, 0x1b, 0x2, 0x117, 0x118, 0x7, 0x8, 
    0x2, 0x2, 0x118, 0x119, 0x5, 0x34, 0x1b, 0x2, 0x119, 0x11a, 0x7, 0x7, 
    0x2, 0x2, 0x11a, 0x11c, 0x3, 0x2, 0x2, 0x2, 0x11b, 0x111, 0x3, 0x2, 
    0x2, 0x2, 0x11b, 0x115, 0x3, 0x2, 0x2, 0x2, 0x11b, 0x11c, 0x3, 0x2, 
    0x2, 0x2, 0x11c, 0x15, 0x3, 0x2, 0x2, 0x2, 0x11d, 0x11e, 0x8, 0xc, 0x1, 
    0x2, 0x11e, 0x16a, 0x5, 0x36, 0x1c, 0x2, 0x11f, 0x16a, 0x7, 0x9a, 0x2, 
    0x2, 0x120, 0x121, 0x5, 0x44, 0x23, 0x2, 0x121, 0x122, 0x7, 0x5, 0x2, 
    0x2, 0x122, 0x124, 0x3, 0x2, 0x2, 0x2, 0x123, 0x120, 0x3, 0x2, 0x2, 
    0x2, 0x123, 0x124, 0x3, 0x2, 0x2, 0x2, 0x124, 0x125, 0x3, 0x2, 0x2, 
    0x2, 0x125, 0x126, 0x5, 0x46, 0x24, 0x2, 0x126, 0x127, 0x7, 0x5, 0x2, 
    0x2, 0x127, 0x129, 0x3, 0x2, 0x2, 0x2, 0x128, 0x123, 0x3, 0x2, 0x2, 
    0x2, 0x128, 0x129, 0x3, 0x2, 0x2, 0x2, 0x129, 0x12a, 0x3, 0x2, 0x2, 
    0x2, 0x12a, 0x16a, 0x5, 0x4c, 0x27, 0x2, 0x12b, 0x12c, 0x5, 0x38, 0x1d, 
    0x2, 0x12c, 0x12d, 0x5, 0x16, 0xc, 0x18, 0x12d, 0x16a, 0x3, 0x2, 0x2, 
    0x2, 0x12e, 0x12f, 0x5, 0x42, 0x22, 0x2, 0x12f, 0x13c, 0x7, 0x6, 0x2, 
    0x2, 0x130, 0x132, 0x7, 0x41, 0x2, 0x2, 0x131, 0x130, 0x3, 0x2, 0x2, 
    0x2, 0x131, 0x132, 0x3, 0x2, 0x2, 0x2, 0x132, 0x133, 0x3, 0x2, 0x2, 
    0x2, 0x133, 0x138, 0x5, 0x16, 0xc, 0x2, 0x134, 0x135, 0x7, 0x8, 0x2, 
    0x2, 0x135, 0x137, 0x5, 0x16, 0xc, 0x2, 0x136, 0x134, 0x3, 0x2, 0x2, 
    0x2, 0x137, 0x13a, 0x3, 0x2, 0x2, 0x2, 0x138, 0x136, 0x3, 0x2, 0x2, 
    0x2, 0x138, 0x139, 0x3, 0x2, 0x2, 0x2, 0x139, 0x13d, 0x3, 0x2, 0x2, 
    0x2, 0x13a, 0x138, 0x3, 0x2, 0x2, 0x2, 0x13b, 0x13d, 0x7, 0xa, 0x2, 
    0x2, 0x13c, 0x131, 0x3, 0x2, 0x2, 0x2, 0x13c, 0x13b, 0x3, 0x2, 0x2, 
    0x2, 0x13c, 0x13d, 0x3, 0x2, 0x2, 0x2, 0x13d, 0x13e, 0x3, 0x2, 0x2, 
    0x2, 0x13e, 0x13f, 0x7, 0x7, 0x2, 0x2, 0x13f, 0x16a, 0x3, 0x2, 0x2, 
    0x2, 0x140, 0x141, 0x7, 0x6, 0x2, 0x2, 0x141, 0x142, 0x5, 0x16, 0xc, 
    0x2, 0x142, 0x143, 0x7, 0x7, 0x2, 0x2, 0x143, 0x16a, 0x3, 0x2, 0x2, 
    0x2, 0x144, 0x145, 0x7, 0x2e, 0x2, 0x2, 0x145, 0x146, 0x7, 0x6, 0x2, 
    0x2, 0x146, 0x147, 0x5, 0x16, 0xc, 0x2, 0x147, 0x148, 0x7, 0x24, 0x2, 
    0x2, 0x148, 0x149, 0x5, 0x14, 0xb, 0x2, 0x149, 0x14a, 0x7, 0x7, 0x2, 
    0x2, 0x14a, 0x16a, 0x3, 0x2, 0x2, 0x2, 0x14b, 0x14d, 0x7, 0x69, 0x2, 
    0x2, 0x14c, 0x14b, 0x3, 0x2, 0x2, 0x2, 0x14c, 0x14d, 0x3, 0x2, 0x2, 
    0x2, 0x14d, 0x14e, 0x3, 0x2, 0x2, 0x2, 0x14e, 0x150, 0x7, 0x49, 0x2, 
    0x2, 0x14f, 0x14c, 0x3, 0x2, 0x2, 0x2, 0x14f, 0x150, 0x3, 0x2, 0x2, 
    0x2, 0x150, 0x151, 0x3, 0x2, 0x2, 0x2, 0x151, 0x152, 0x7, 0x6, 0x2, 
    0x2, 0x152, 0x153, 0x5, 0xa, 0x6, 0x2, 0x153, 0x154, 0x7, 0x7, 0x2, 
    0x2, 0x154, 0x16a, 0x3, 0x2, 0x2, 0x2, 0x155, 0x157, 0x7, 0x2d, 0x2, 
    0x2, 0x156, 0x158, 0x5, 0x16, 0xc, 0x2, 0x157, 0x156, 0x3, 0x2, 0x2, 
    0x2, 0x157, 0x158, 0x3, 0x2, 0x2, 0x2, 0x158, 0x15e, 0x3, 0x2, 0x2, 
    0x2, 0x159, 0x15a, 0x7, 0x94, 0x2, 0x2, 0x15a, 0x15b, 0x5, 0x16, 0xc, 
    0x2, 0x15b, 0x15c, 0x7, 0x88, 0x2, 0x2, 0x15c, 0x15d, 0x5, 0x16, 0xc, 
    0x2, 0x15d, 0x15f, 0x3, 0x2, 0x2, 0x2, 0x15e, 0x159, 0x3, 0x2, 0x2, 
    0x2, 0x15f, 0x160, 0x3, 0x2, 0x2, 0x2, 0x160, 0x15e, 0x3, 0x2, 0x2, 
    0x2, 0x160, 0x161, 0x3, 0x2, 0x2, 0x2, 0x161, 0x164, 0x3, 0x2, 0x2, 
    0x2, 0x162, 0x163, 0x7, 0x44, 0x2, 0x2, 0x163, 0x165, 0x5, 0x16, 0xc, 
    0x2, 0x164, 0x162, 0x3, 0x2, 0x2, 0x2, 0x164, 0x165, 0x3, 0x2, 0x2, 
    0x2, 0x165, 0x166, 0x3, 0x2, 0x2, 0x2, 0x166, 0x167, 0x7, 0x45, 0x2, 
    0x2, 0x167, 0x16a, 0x3, 0x2, 0x2, 0x2, 0x168, 0x16a, 0x5, 0x18, 0xd, 
    0x2, 0x169, 0x11d, 0x3, 0x2, 0x2, 0x2, 0x169, 0x11f, 0x3, 0x2, 0x2, 
    0x2, 0x169, 0x128, 0x3, 0x2, 0x2, 0x2, 0x169, 0x12b, 0x3, 0x2, 0x2, 
    0x2, 0x169, 0x12e, 0x3, 0x2, 0x2, 0x2, 0x169, 0x140, 0x3, 0x2, 0x2, 
    0x2, 0x169, 0x144, 0x3, 0x2, 0x2, 0x2, 0x169, 0x14f, 0x3, 0x2, 0x2, 
    0x2, 0x169, 0x155, 0x3, 0x2, 0x2, 0x2, 0x169, 0x168, 0x3, 0x2, 0x2, 
    0x2, 0x16a, 0x1c5, 0x3, 0x2, 0x2, 0x2, 0x16b, 0x16c, 0xc, 0x17, 0x2, 
    0x2, 0x16c, 0x16d, 0x9, 0x4, 0x2, 0x2, 0x16d, 0x1c4, 0x5, 0x16, 0xc, 
    0x18, 0x16e, 0x16f, 0xc, 0x16, 0x2, 0x2, 0x16f, 0x170, 0x9, 0x5, 0x2, 
    0x2, 0x170, 0x1c4, 0x5, 0x16, 0xc, 0x17, 0x171, 0x172, 0xc, 0x15, 0x2, 
    0x2, 0x172, 0x173, 0x9, 0x6, 0x2, 0x2, 0x173, 0x1c4, 0x5, 0x16, 0xc, 
    0x16, 0x174, 0x175, 0xc, 0x14, 0x2, 0x2, 0x175, 0x176, 0x9, 0x7, 0x2, 
    0x2, 0x176, 0x1c4, 0x5, 0x16, 0xc, 0x15, 0x177, 0x179, 0xc, 0x13, 0x2, 
    0x2, 0x178, 0x17a, 0x7, 0x69, 0x2, 0x2, 0x179, 0x178, 0x3, 0x2, 0x2, 
    0x2, 0x179, 0x17a, 0x3, 0x2, 0x2, 0x2, 0x17a, 0x17b, 0x3, 0x2, 0x2, 
    0x2, 0x17b, 0x17c, 0x7, 0x2a, 0x2, 0x2, 0x17c, 0x17d, 0x5, 0x16, 0xc, 
    0x2, 0x17d, 0x17e, 0x7, 0x23, 0x2, 0x2, 0x17e, 0x17f, 0x5, 0x16, 0xc, 
    0x14, 0x17f, 0x1c4, 0x3, 0x2, 0x2, 0x2, 0x180, 0x181, 0xc, 0x12, 0x2, 
    0x2, 0x181, 0x182, 0x9, 0x8, 0x2, 0x2, 0x182, 0x1c4, 0x5, 0x16, 0xc, 
    0x13, 0x183, 0x184, 0xc, 0xb, 0x2, 0x2, 0x184, 0x186, 0x7, 0x5f, 0x2, 
    0x2, 0x185, 0x187, 0x7, 0x69, 0x2, 0x2, 0x186, 0x185, 0x3, 0x2, 0x2, 
    0x2, 0x186, 0x187, 0x3, 0x2, 0x2, 0x2, 0x187, 0x188, 0x3, 0x2, 0x2, 
    0x2, 0x188, 0x1c4, 0x5, 0x16, 0xc, 0xc, 0x189, 0x18a, 0xc, 0x6, 0x2, 
    0x2, 0x18a, 0x18b, 0x7, 0x23, 0x2, 0x2, 0x18b, 0x1c4, 0x5, 0x16, 0xc, 
    0x7, 0x18c, 0x18d, 0xc, 0x5, 0x2, 0x2, 0x18d, 0x18e, 0x7, 0x3, 0x2, 
    0x2, 0x18e, 0x1c4, 0x5, 0x16, 0xc, 0x6, 0x18f, 0x190, 0xc, 0x4, 0x2, 
    0x2, 0x190, 0x191, 0x7, 0x6f, 0x2, 0x2, 0x191, 0x1c4, 0x5, 0x16, 0xc, 
    0x5, 0x192, 0x193, 0xc, 0x3, 0x2, 0x2, 0x193, 0x194, 0x7, 0xe, 0x2, 
    0x2, 0x194, 0x1c4, 0x5, 0x16, 0xc, 0x4, 0x195, 0x196, 0xc, 0xe, 0x2, 
    0x2, 0x196, 0x197, 0x7, 0x30, 0x2, 0x2, 0x197, 0x1c4, 0x5, 0x4e, 0x28, 
    0x2, 0x198, 0x19a, 0xc, 0xd, 0x2, 0x2, 0x199, 0x19b, 0x7, 0x69, 0x2, 
    0x2, 0x19a, 0x199, 0x3, 0x2, 0x2, 0x2, 0x19a, 0x19b, 0x3, 0x2, 0x2, 
    0x2, 0x19b, 0x19c, 0x3, 0x2, 0x2, 0x2, 0x19c, 0x19d, 0x9, 0x9, 0x2, 
    0x2, 0x19d, 0x1a0, 0x5, 0x16, 0xc, 0x2, 0x19e, 0x19f, 0x7, 0x46, 0x2, 
    0x2, 0x19f, 0x1a1, 0x5, 0x16, 0xc, 0x2, 0x1a0, 0x19e, 0x3, 0x2, 0x2, 
    0x2, 0x1a0, 0x1a1, 0x3, 0x2, 0x2, 0x2, 0x1a1, 0x1c4, 0x3, 0x2, 0x2, 
    0x2, 0x1a2, 0x1a7, 0xc, 0xc, 0x2, 0x2, 0x1a3, 0x1a8, 0x7, 0x60, 0x2, 
    0x2, 0x1a4, 0x1a8, 0x7, 0x6a, 0x2, 0x2, 0x1a5, 0x1a6, 0x7, 0x69, 0x2, 
    0x2, 0x1a6, 0x1a8, 0x7, 0x6b, 0x2, 0x2, 0x1a7, 0x1a3, 0x3, 0x2, 0x2, 
    0x2, 0x1a7, 0x1a4, 0x3, 0x2, 0x2, 0x2, 0x1a7, 0x1a5, 0x3, 0x2, 0x2, 
    0x2, 0x1a8, 0x1c4, 0x3, 0x2, 0x2, 0x2, 0x1a9, 0x1ab, 0xc, 0xa, 0x2, 
    0x2, 0x1aa, 0x1ac, 0x7, 0x69, 0x2, 0x2, 0x1ab, 0x1aa, 0x3, 0x2, 0x2, 
    0x2, 0x1ab, 0x1ac, 0x3, 0x2, 0x2, 0x2, 0x1ac, 0x1ad, 0x3, 0x2, 0x2, 
    0x2, 0x1ad, 0x1c1, 0x7, 0x56, 0x2, 0x2, 0x1ae, 0x1b8, 0x7, 0x6, 0x2, 
    0x2, 0x1af, 0x1b9, 0x5, 0xa, 0x6, 0x2, 0x1b0, 0x1b5, 0x5, 0x16, 0xc, 
    0x2, 0x1b1, 0x1b2, 0x7, 0x8, 0x2, 0x2, 0x1b2, 0x1b4, 0x5, 0x16, 0xc, 
    0x2, 0x1b3, 0x1b1, 0x3, 0x2, 0x2, 0x2, 0x1b4, 0x1b7, 0x3, 0x2, 0x2, 
    0x2, 0x1b5, 0x1b3, 0x3, 0x2, 0x2, 0x2, 0x1b5, 0x1b6, 0x3, 0x2, 0x2, 
    0x2, 0x1b6, 0x1b9, 0x3, 0x2, 0x2, 0x2, 0x1b7, 0x1b5, 0x3, 0x2, 0x2, 
    0x2, 0x1b8, 0x1af, 0x3, 0x2, 0x2, 0x2, 0x1b8, 0x1b0, 0x3, 0x2, 0x2, 
    0x2, 0x1b8, 0x1b9, 0x3, 0x2, 0x2, 0x2, 0x1b9, 0x1ba, 0x3, 0x2, 0x2, 
    0x2, 0x1ba, 0x1c2, 0x7, 0x7, 0x2, 0x2, 0x1bb, 0x1bc, 0x5, 0x44, 0x23, 
    0x2, 0x1bc, 0x1bd, 0x7, 0x5, 0x2, 0x2, 0x1bd, 0x1bf, 0x3, 0x2, 0x2, 
    0x2, 0x1be, 0x1bb, 0x3, 0x2, 0x2, 0x2, 0x1be, 0x1bf, 0x3, 0x2, 0x2, 
    0x2, 0x1bf, 0x1c0, 0x3, 0x2, 0x2, 0x2, 0x1c0, 0x1c2, 0x5, 0x46, 0x24, 
    0x2, 0x1c1, 0x1ae, 0x3, 0x2, 0x2, 0x2, 0x1c1, 0x1be, 0x3, 0x2, 0x2, 
    0x2, 0x1c2, 0x1c4, 0x3, 0x2, 0x2, 0x2, 0x1c3, 0x16b, 0x3, 0x2, 0x2, 
    0x2, 0x1c3, 0x16e, 0x3, 0x2, 0x2, 0x2, 0x1c3, 0x171, 0x3, 0x2, 0x2, 
    0x2, 0x1c3, 0x174, 0x3, 0x2, 0x2, 0x2, 0x1c3, 0x177, 0x3, 0x2, 0x2, 
    0x2, 0x1c3, 0x180, 0x3, 0x2, 0x2, 0x2, 0x1c3, 0x183, 0x3, 0x2, 0x2, 
    0x2, 0x1c3, 0x189, 0x3, 0x2, 0x2, 0x2, 0x1c3, 0x18c, 0x3, 0x2, 0x2, 
    0x2, 0x1c3, 0x18f, 0x3, 0x2, 0x2, 0x2, 0x1c3, 0x192, 0x3, 0x2, 0x2, 
    0x2, 0x1c3, 0x195, 0x3, 0x2, 0x2, 0x2, 0x1c3, 0x198, 0x3, 0x2, 0x2, 
    0x2, 0x1c3, 0x1a2, 0x3, 0x2, 0x2, 0x2, 0x1c3, 0x1a9, 0x3, 0x2, 0x2, 
    0x2, 0x1c4, 0x1c7, 0x3, 0x2, 0x2, 0x2, 0x1c5, 0x1c3, 0x3, 0x2, 0x2, 
    0x2, 0x1c5, 0x1c6, 0x3, 0x2, 0x2, 0x2, 0x1c6, 0x17, 0x3, 0x2, 0x2, 0x2, 
    0x1c7, 0x1c5, 0x3, 0x2, 0x2, 0x2, 0x1c8, 0x1c9, 0x7, 0x76, 0x2, 0x2, 
    0x1c9, 0x1ce, 0x7, 0x6, 0x2, 0x2, 0x1ca, 0x1cf, 0x7, 0x54, 0x2, 0x2, 
    0x1cb, 0x1cc, 0x9, 0xa, 0x2, 0x2, 0x1cc, 0x1cd, 0x7, 0x8, 0x2, 0x2, 
    0x1cd, 0x1cf, 0x5, 0x3a, 0x1e, 0x2, 0x1ce, 0x1ca, 0x3, 0x2, 0x2, 0x2, 
    0x1ce, 0x1cb, 0x3, 0x2, 0x2, 0x2, 0x1cf, 0x1d0, 0x3, 0x2, 0x2, 0x2, 
    0x1d0, 0x1d1, 0x7, 0x7, 0x2, 0x2, 0x1d1, 0x19, 0x3, 0x2, 0x2, 0x2, 0x1d2, 
    0x1d5, 0x5, 0x4c, 0x27, 0x2, 0x1d3, 0x1d4, 0x7, 0x30, 0x2, 0x2, 0x1d4, 
    0x1d6, 0x5, 0x4e, 0x28, 0x2, 0x1d5, 0x1d3, 0x3, 0x2, 0x2, 0x2, 0x1d5, 
    0x1d6, 0x3, 0x2, 0x2, 0x2, 0x1d6, 0x1d8, 0x3, 0x2, 0x2, 0x2, 0x1d7, 
    0x1d9, 0x9, 0xb, 0x2, 0x2, 0x1d8, 0x1d7, 0x3, 0x2, 0x2, 0x2, 0x1d8, 
    0x1d9, 0x3, 0x2, 0x2, 0x2, 0x1d9, 0x1b, 0x3, 0x2, 0x2, 0x2, 0x1da, 0x1dc, 
    0x7, 0x96, 0x2, 0x2, 0x1db, 0x1dd, 0x7, 0x77, 0x2, 0x2, 0x1dc, 0x1db, 
    0x3, 0x2, 0x2, 0x2, 0x1dc, 0x1dd, 0x3, 0x2, 0x2, 0x2, 0x1dd, 0x1de, 
    0x3, 0x2, 0x2, 0x2, 0x1de, 0x1df, 0x5, 0x32, 0x1a, 0x2, 0x1df, 0x1e0, 
    0x7, 0x24, 0x2, 0x2, 0x1e0, 0x1e1, 0x7, 0x6, 0x2, 0x2, 0x1e1, 0x1e2, 
    0x5, 0xa, 0x6, 0x2, 0x1e2, 0x1ec, 0x7, 0x7, 0x2, 0x2, 0x1e3, 0x1e4, 
    0x7, 0x8, 0x2, 0x2, 0x1e4, 0x1e5, 0x5, 0x32, 0x1a, 0x2, 0x1e5, 0x1e6, 
    0x7, 0x24, 0x2, 0x2, 0x1e6, 0x1e7, 0x7, 0x6, 0x2, 0x2, 0x1e7, 0x1e8, 
    0x5, 0xa, 0x6, 0x2, 0x1e8, 0x1e9, 0x7, 0x7, 0x2, 0x2, 0x1e9, 0x1eb, 
    0x3, 0x2, 0x2, 0x2, 0x1ea, 0x1e3, 0x3, 0x2, 0x2, 0x2, 0x1eb, 0x1ee, 
    0x3, 0x2, 0x2, 0x2, 0x1ec, 0x1ea, 0x3, 0x2, 0x2, 0x2, 0x1ec, 0x1ed, 
    0x3, 0x2, 0x2, 0x2, 0x1ed, 0x1d, 0x3, 0x2, 0x2, 0x2, 0x1ee, 0x1ec, 0x3, 
    0x2, 0x2, 0x2, 0x1ef, 0x1f0, 0x5, 0x44, 0x23, 0x2, 0x1f0, 0x1f1, 0x7, 
    0x5, 0x2, 0x2, 0x1f1, 0x1f3, 0x3, 0x2, 0x2, 0x2, 0x1f2, 0x1ef, 0x3, 
    0x2, 0x2, 0x2, 0x1f2, 0x1f3, 0x3, 0x2, 0x2, 0x2, 0x1f3, 0x1f4, 0x3, 
    0x2, 0x2, 0x2, 0x1f4, 0x1fa, 0x5, 0x46, 0x24, 0x2, 0x1f5, 0x1f6, 0x7, 
    0x58, 0x2, 0x2, 0x1f6, 0x1f7, 0x7, 0x2b, 0x2, 0x2, 0x1f7, 0x1fb, 0x5, 
    0x52, 0x2a, 0x2, 0x1f8, 0x1f9, 0x7, 0x69, 0x2, 0x2, 0x1f9, 0x1fb, 0x7, 
    0x58, 0x2, 0x2, 0x1fa, 0x1f5, 0x3, 0x2, 0x2, 0x2, 0x1fa, 0x1f8, 0x3, 
    0x2, 0x2, 0x2, 0x1fa, 0x1fb, 0x3, 0x2, 0x2, 0x2, 0x1fb, 0x1f, 0x3, 0x2, 
    0x2, 0x2, 0x1fc, 0x1ff, 0x5, 0x16, 0xc, 0x2, 0x1fd, 0x1fe, 0x7, 0x30, 
    0x2, 0x2, 0x1fe, 0x200, 0x5, 0x4e, 0x28, 0x2, 0x1ff, 0x1fd, 0x3, 0x2, 
    0x2, 0x2, 0x1ff, 0x200, 0x3, 0x2, 0x2, 0x2, 0x200, 0x202, 0x3, 0x2, 
    0x2, 0x2, 0x201, 0x203, 0x9, 0xb, 0x2, 0x2, 0x202, 0x201, 0x3, 0x2, 
    0x2, 0x2, 0x202, 0x203, 0x3, 0x2, 0x2, 0x2, 0x203, 0x21, 0x3, 0x2, 0x2, 
    0x2, 0x204, 0x208, 0x5, 0x34, 0x1b, 0x2, 0x205, 0x208, 0x5, 0x40, 0x21, 
    0x2, 0x206, 0x208, 0x7, 0x9b, 0x2, 0x2, 0x207, 0x204, 0x3, 0x2, 0x2, 
    0x2, 0x207, 0x205, 0x3, 0x2, 0x2, 0x2, 0x207, 0x206, 0x3, 0x2, 0x2, 
    0x2, 0x208, 0x23, 0x3, 0x2, 0x2, 0x2, 0x209, 0x215, 0x5, 0x46, 0x24, 
    0x2, 0x20a, 0x20b, 0x7, 0x6, 0x2, 0x2, 0x20b, 0x210, 0x5, 0x4c, 0x27, 
    0x2, 0x20c, 0x20d, 0x7, 0x8, 0x2, 0x2, 0x20d, 0x20f, 0x5, 0x4c, 0x27, 
    0x2, 0x20e, 0x20c, 0x3, 0x2, 0x2, 0x2, 0x20f, 0x212, 0x3, 0x2, 0x2, 
    0x2, 0x210, 0x20e, 0x3, 0x2, 0x2, 0x2, 0x210, 0x211, 0x3, 0x2, 0x2, 
    0x2, 0x211, 0x213, 0x3, 0x2, 0x2, 0x2, 0x212, 0x210, 0x3, 0x2, 0x2, 
    0x2, 0x213, 0x214, 0x7, 0x7, 0x2, 0x2, 0x214, 0x216, 0x3, 0x2, 0x2, 
    0x2, 0x215, 0x20a, 0x3, 0x2, 0x2, 0x2, 0x215, 0x216, 0x3, 0x2, 0x2, 
    0x2, 0x216, 0x217, 0x3, 0x2, 0x2, 0x2, 0x217, 0x218, 0x7, 0x24, 0x2, 
    0x2, 0x218, 0x219, 0x7, 0x6, 0x2, 0x2, 0x219, 0x21a, 0x5, 0xa, 0x6, 
    0x2, 0x21a, 0x21b, 0x7, 0x7, 0x2, 0x2, 0x21b, 0x25, 0x3, 0x2, 0x2, 0x2, 
    0x21c, 0x229, 0x7, 0xa, 0x2, 0x2, 0x21d, 0x21e, 0x5, 0x46, 0x24, 0x2, 
    0x21e, 0x21f, 0x7, 0x5, 0x2, 0x2, 0x21f, 0x220, 0x7, 0xa, 0x2, 0x2, 
    0x220, 0x229, 0x3, 0x2, 0x2, 0x2, 0x221, 0x226, 0x5, 0x16, 0xc, 0x2, 
    0x222, 0x224, 0x7, 0x24, 0x2, 0x2, 0x223, 0x222, 0x3, 0x2, 0x2, 0x2, 
    0x223, 0x224, 0x3, 0x2, 0x2, 0x2, 0x224, 0x225, 0x3, 0x2, 0x2, 0x2, 
    0x225, 0x227, 0x5, 0x3c, 0x1f, 0x2, 0x226, 0x223, 0x3, 0x2, 0x2, 0x2, 
    0x226, 0x227, 0x3, 0x2, 0x2, 0x2, 0x227, 0x229, 0x3, 0x2, 0x2, 0x2, 
    0x228, 0x21c, 0x3, 0x2, 0x2, 0x2, 0x228, 0x21d, 0x3, 0x2, 0x2, 0x2, 
    0x228, 0x221, 0x3, 0x2, 0x2, 0x2, 0x229, 0x27, 0x3, 0x2, 0x2, 0x2, 0x22a, 
    0x22b, 0x5, 0x44, 0x23, 0x2, 0x22b, 0x22c, 0x7, 0x5, 0x2, 0x2, 0x22c, 
    0x22e, 0x3, 0x2, 0x2, 0x2, 0x22d, 0x22a, 0x3, 0x2, 0x2, 0x2, 0x22d, 
    0x22e, 0x3, 0x2, 0x2, 0x2, 0x22e, 0x22f, 0x3, 0x2, 0x2, 0x2, 0x22f, 
    0x234, 0x5, 0x46, 0x24, 0x2, 0x230, 0x232, 0x7, 0x24, 0x2, 0x2, 0x231, 
    0x230, 0x3, 0x2, 0x2, 0x2, 0x231, 0x232, 0x3, 0x2, 0x2, 0x2, 0x232, 
    0x233, 0x3, 0x2, 0x2, 0x2, 0x233, 0x235, 0x5, 0x5e, 0x30, 0x2, 0x234, 
    0x231, 0x3, 0x2, 0x2, 0x2, 0x234, 0x235, 0x3, 0x2, 0x2, 0x2, 0x235, 
    0x23b, 0x3, 0x2, 0x2, 0x2, 0x236, 0x237, 0x7, 0x58, 0x2, 0x2, 0x237, 
    0x238, 0x7, 0x2b, 0x2, 0x2, 0x238, 0x23c, 0x5, 0x52, 0x2a, 0x2, 0x239, 
    0x23a, 0x7, 0x69, 0x2, 0x2, 0x23a, 0x23c, 0x7, 0x58, 0x2, 0x2, 0x23b, 
    0x236, 0x3, 0x2, 0x2, 0x2, 0x23b, 0x239, 0x3, 0x2, 0x2, 0x2, 0x23b, 
    0x23c, 0x3, 0x2, 0x2, 0x2, 0x23c, 0x25a, 0x3, 0x2, 0x2, 0x2, 0x23d, 
    0x247, 0x7, 0x6, 0x2, 0x2, 0x23e, 0x243, 0x5, 0x28, 0x15, 0x2, 0x23f, 
    0x240, 0x7, 0x8, 0x2, 0x2, 0x240, 0x242, 0x5, 0x28, 0x15, 0x2, 0x241, 
    0x23f, 0x3, 0x2, 0x2, 0x2, 0x242, 0x245, 0x3, 0x2, 0x2, 0x2, 0x243, 
    0x241, 0x3, 0x2, 0x2, 0x2, 0x243, 0x244, 0x3, 0x2, 0x2, 0x2, 0x244, 
    0x248, 0x3, 0x2, 0x2, 0x2, 0x245, 0x243, 0x3, 0x2, 0x2, 0x2, 0x246, 
    0x248, 0x5, 0x2a, 0x16, 0x2, 0x247, 0x23e, 0x3, 0x2, 0x2, 0x2, 0x247, 
    0x246, 0x3, 0x2, 0x2, 0x2, 0x248, 0x249, 0x3, 0x2, 0x2, 0x2, 0x249, 
    0x24e, 0x7, 0x7, 0x2, 0x2, 0x24a, 0x24c, 0x7, 0x24, 0x2, 0x2, 0x24b, 
    0x24a, 0x3, 0x2, 0x2, 0x2, 0x24b, 0x24c, 0x3, 0x2, 0x2, 0x2, 0x24c, 
    0x24d, 0x3, 0x2, 0x2, 0x2, 0x24d, 0x24f, 0x5, 0x5e, 0x30, 0x2, 0x24e, 
    0x24b, 0x3, 0x2, 0x2, 0x2, 0x24e, 0x24f, 0x3, 0x2, 0x2, 0x2, 0x24f, 
    0x25a, 0x3, 0x2, 0x2, 0x2, 0x250, 0x251, 0x7, 0x6, 0x2, 0x2, 0x251, 
    0x252, 0x5, 0xa, 0x6, 0x2, 0x252, 0x257, 0x7, 0x7, 0x2, 0x2, 0x253, 
    0x255, 0x7, 0x24, 0x2, 0x2, 0x254, 0x253, 0x3, 0x2, 0x2, 0x2, 0x254, 
    0x255, 0x3, 0x2, 0x2, 0x2, 0x255, 0x256, 0x3, 0x2, 0x2, 0x2, 0x256, 
    0x258, 0x5, 0x5e, 0x30, 0x2, 0x257, 0x254, 0x3, 0x2, 0x2, 0x2, 0x257, 
    0x258, 0x3, 0x2, 0x2, 0x2, 0x258, 0x25a, 0x3, 0x2, 0x2, 0x2, 0x259, 
    0x22d, 0x3, 0x2, 0x2, 0x2, 0x259, 0x23d, 0x3, 0x2, 0x2, 0x2, 0x259, 
    0x250, 0x3, 0x2, 0x2, 0x2, 0x25a, 0x29, 0x3, 0x2, 0x2, 0x2, 0x25b, 0x262, 
    0x5, 0x28, 0x15, 0x2, 0x25c, 0x25d, 0x5, 0x2c, 0x17, 0x2, 0x25d, 0x25e, 
    0x5, 0x28, 0x15, 0x2, 0x25e, 0x25f, 0x5, 0x2e, 0x18, 0x2, 0x25f, 0x261, 
    0x3, 0x2, 0x2, 0x2, 0x260, 0x25c, 0x3, 0x2, 0x2, 0x2, 0x261, 0x264, 
    0x3, 0x2, 0x2, 0x2, 0x262, 0x260, 0x3, 0x2, 0x2, 0x2, 0x262, 0x263, 
    0x3, 0x2, 0x2, 0x2, 0x263, 0x2b, 0x3, 0x2, 0x2, 0x2, 0x264, 0x262, 0x3, 
    0x2, 0x2, 0x2, 0x265, 0x273, 0x7, 0x8, 0x2, 0x2, 0x266, 0x268, 0x7, 
    0x67, 0x2, 0x2, 0x267, 0x266, 0x3, 0x2, 0x2, 0x2, 0x267, 0x268, 0x3, 
    0x2, 0x2, 0x2, 0x268, 0x26f, 0x3, 0x2, 0x2, 0x2, 0x269, 0x26b, 0x7, 
    0x63, 0x2, 0x2, 0x26a, 0x26c, 0x7, 0x71, 0x2, 0x2, 0x26b, 0x26a, 0x3, 
    0x2, 0x2, 0x2, 0x26b, 0x26c, 0x3, 0x2, 0x2, 0x2, 0x26c, 0x270, 0x3, 
    0x2, 0x2, 0x2, 0x26d, 0x270, 0x7, 0x5a, 0x2, 0x2, 0x26e, 0x270, 0x7, 
    0x36, 0x2, 0x2, 0x26f, 0x269, 0x3, 0x2, 0x2, 0x2, 0x26f, 0x26d, 0x3, 
    0x2, 0x2, 0x2, 0x26f, 0x26e, 0x3, 0x2, 0x2, 0x2, 0x26f, 0x270, 0x3, 
    0x2, 0x2, 0x2, 0x270, 0x271, 0x3, 0x2, 0x2, 0x2, 0x271, 0x273, 0x7, 
    0x61, 0x2, 0x2, 0x272, 0x265, 0x3, 0x2, 0x2, 0x2, 0x272, 0x267, 0x3, 
    0x2, 0x2, 0x2, 0x273, 0x2d, 0x3, 0x2, 0x2, 0x2, 0x274, 0x275, 0x7, 0x6e, 
    0x2, 0x2, 0x275, 0x283, 0x5, 0x16, 0xc, 0x2, 0x276, 0x277, 0x7, 0x8f, 
    0x2, 0x2, 0x277, 0x278, 0x7, 0x6, 0x2, 0x2, 0x278, 0x27d, 0x5, 0x4c, 
    0x27, 0x2, 0x279, 0x27a, 0x7, 0x8, 0x2, 0x2, 0x27a, 0x27c, 0x5, 0x4c, 
    0x27, 0x2, 0x27b, 0x279, 0x3, 0x2, 0x2, 0x2, 0x27c, 0x27f, 0x3, 0x2, 
    0x2, 0x2, 0x27d, 0x27b, 0x3, 0x2, 0x2, 0x2, 0x27d, 0x27e, 0x3, 0x2, 
    0x2, 0x2, 0x27e, 0x280, 0x3, 0x2, 0x2, 0x2, 0x27f, 0x27d, 0x3, 0x2, 
    0x2, 0x2, 0x280, 0x281, 0x7, 0x7, 0x2, 0x2, 0x281, 0x283, 0x3, 0x2, 
    0x2, 0x2, 0x282, 0x274, 0x3, 0x2, 0x2, 0x2, 0x282, 0x276, 0x3, 0x2, 
    0x2, 0x2, 0x282, 0x283, 0x3, 0x2, 0x2, 0x2, 0x283, 0x2f, 0x3, 0x2, 0x2, 
    0x2, 0x284, 0x28a, 0x7, 0x8c, 0x2, 0x2, 0x285, 0x286, 0x7, 0x8c, 0x2, 
    0x2, 0x286, 0x28a, 0x7, 0x20, 0x2, 0x2, 0x287, 0x28a, 0x7, 0x5d, 0x2, 
    0x2, 0x288, 0x28a, 0x7, 0x47, 0x2, 0x2, 0x289, 0x284, 0x3, 0x2, 0x2, 
    0x2, 0x289, 0x285, 0x3, 0x2, 0x2, 0x2, 0x289, 0x287, 0x3, 0x2, 0x2, 
    0x2, 0x289, 0x288, 0x3, 0x2, 0x2, 0x2, 0x28a, 0x31, 0x3, 0x2, 0x2, 0x2, 
    0x28b, 0x297, 0x5, 0x46, 0x24, 0x2, 0x28c, 0x28d, 0x7, 0x6, 0x2, 0x2, 
    0x28d, 0x292, 0x5, 0x4c, 0x27, 0x2, 0x28e, 0x28f, 0x7, 0x8, 0x2, 0x2, 
    0x28f, 0x291, 0x5, 0x4c, 0x27, 0x2, 0x290, 0x28e, 0x3, 0x2, 0x2, 0x2, 
    0x291, 0x294, 0x3, 0x2, 0x2, 0x2, 0x292, 0x290, 0x3, 0x2, 0x2, 0x2, 
    0x292, 0x293, 0x3, 0x2, 0x2, 0x2, 0x293, 0x295, 0x3, 0x2, 0x2, 0x2, 
    0x294, 0x292, 0x3, 0x2, 0x2, 0x2, 0x295, 0x296, 0x7, 0x7, 0x2, 0x2, 
    0x296, 0x298, 0x3, 0x2, 0x2, 0x2, 0x297, 0x28c, 0x3, 0x2, 0x2, 0x2, 
    0x297, 0x298, 0x3, 0x2, 0x2, 0x2, 0x298, 0x33, 0x3, 0x2, 0x2, 0x2, 0x299, 
    0x29b, 0x9, 0x5, 0x2, 0x2, 0x29a, 0x299, 0x3, 0x2, 0x2, 0x2, 0x29a, 
    0x29b, 0x3, 0x2, 0x2, 0x2, 0x29b, 0x29c, 0x3, 0x2, 0x2, 0x2, 0x29c, 
    0x29d, 0x7, 0x99, 0x2, 0x2, 0x29d, 0x35, 0x3, 0x2, 0x2, 0x2, 0x29e, 
    0x2a6, 0x7, 0x99, 0x2, 0x2, 0x29f, 0x2a6, 0x7, 0x9b, 0x2, 0x2, 0x2a0, 
    0x2a6, 0x7, 0x9c, 0x2, 0x2, 0x2a1, 0x2a6, 0x7, 0x6b, 0x2, 0x2, 0x2a2, 
    0x2a6, 0x7, 0x38, 0x2, 0x2, 0x2a3, 0x2a6, 0x7, 0x37, 0x2, 0x2, 0x2a4, 
    0x2a6, 0x7, 0x39, 0x2, 0x2, 0x2a5, 0x29e, 0x3, 0x2, 0x2, 0x2, 0x2a5, 
    0x29f, 0x3, 0x2, 0x2, 0x2, 0x2a5, 0x2a0, 0x3, 0x2, 0x2, 0x2, 0x2a5, 
    0x2a1, 0x3, 0x2, 0x2, 0x2, 0x2a5, 0x2a2, 0x3, 0x2, 0x2, 0x2, 0x2a5, 
    0x2a3, 0x3, 0x2, 0x2, 0x2, 0x2a5, 0x2a4, 0x3, 0x2, 0x2, 0x2, 0x2a6, 
    0x37, 0x3, 0x2, 0x2, 0x2, 0x2a7, 0x2a8, 0x9, 0xc, 0x2, 0x2, 0x2a8, 0x39, 
    0x3, 0x2, 0x2, 0x2, 0x2a9, 0x2aa, 0x7, 0x9b, 0x2, 0x2, 0x2aa, 0x3b, 
    0x3, 0x2, 0x2, 0x2, 0x2ab, 0x2ac, 0x9, 0xd, 0x2, 0x2, 0x2ac, 0x3d, 0x3, 
    0x2, 0x2, 0x2, 0x2ad, 0x2ae, 0x9, 0xe, 0x2, 0x2, 0x2ae, 0x3f, 0x3, 0x2, 
    0x2, 0x2, 0x2af, 0x2b0, 0x5, 0x62, 0x32, 0x2, 0x2b0, 0x41, 0x3, 0x2, 
    0x2, 0x2, 0x2b1, 0x2b2, 0x5, 0x62, 0x32, 0x2, 0x2b2, 0x43, 0x3, 0x2, 
    0x2, 0x2, 0x2b3, 0x2b4, 0x5, 0x62, 0x32, 0x2, 0x2b4, 0x45, 0x3, 0x2, 
    0x2, 0x2, 0x2b5, 0x2b6, 0x5, 0x62, 0x32, 0x2, 0x2b6, 0x47, 0x3, 0x2, 
    0x2, 0x2, 0x2b7, 0x2b8, 0x5, 0x62, 0x32, 0x2, 0x2b8, 0x49, 0x3, 0x2, 
    0x2, 0x2, 0x2b9, 0x2ba, 0x5, 0x62, 0x32, 0x2, 0x2ba, 0x4b, 0x3, 0x2, 
    0x2, 0x2, 0x2bb, 0x2bc, 0x5, 0x62, 0x32, 0x2, 0x2bc, 0x4d, 0x3, 0x2, 
    0x2, 0x2, 0x2bd, 0x2be, 0x5, 0x62, 0x32, 0x2, 0x2be, 0x4f, 0x3, 0x2, 
    0x2, 0x2, 0x2bf, 0x2c0, 0x5, 0x62, 0x32, 0x2, 0x2c0, 0x51, 0x3, 0x2, 
    0x2, 0x2, 0x2c1, 0x2c2, 0x5, 0x62, 0x32, 0x2, 0x2c2, 0x53, 0x3, 0x2, 
    0x2, 0x2, 0x2c3, 0x2c4, 0x5, 0x62, 0x32, 0x2, 0x2c4, 0x55, 0x3, 0x2, 
    0x2, 0x2, 0x2c5, 0x2c6, 0x5, 0x62, 0x32, 0x2, 0x2c6, 0x57, 0x3, 0x2, 
    0x2, 0x2, 0x2c7, 0x2c8, 0x5, 0x62, 0x32, 0x2, 0x2c8, 0x59, 0x3, 0x2, 
    0x2, 0x2, 0x2c9, 0x2ca, 0x5, 0x62, 0x32, 0x2, 0x2ca, 0x5b, 0x3, 0x2, 
    0x2, 0x2, 0x2cb, 0x2cc, 0x5, 0x62, 0x32, 0x2, 0x2cc, 0x5d, 0x3, 0x2, 
    0x2, 0x2, 0x2cd, 0x2ce, 0x5, 0x62, 0x32, 0x2, 0x2ce, 0x5f, 0x3, 0x2, 
    0x2, 0x2, 0x2cf, 0x2d0, 0x5, 0x62, 0x32, 0x2, 0x2d0, 0x61, 0x3, 0x2, 
    0x2, 0x2, 0x2d1, 0x2d9, 0x7, 0x98, 0x2, 0x2, 0x2d2, 0x2d9, 0x5, 0x3e, 
    0x20, 0x2, 0x2d3, 0x2d9, 0x7, 0x9b, 0x2, 0x2, 0x2d4, 0x2d5, 0x7, 0x6, 
    0x2, 0x2, 0x2d5, 0x2d6, 0x5, 0x62, 0x32, 0x2, 0x2d6, 0x2d7, 0x7, 0x7, 
    0x2, 0x2, 0x2d7, 0x2d9, 0x3, 0x2, 0x2, 0x2, 0x2d8, 0x2d1, 0x3, 0x2, 
    0x2, 0x2, 0x2d8, 0x2d2, 0x3, 0x2, 0x2, 0x2, 0x2d8, 0x2d3, 0x3, 0x2, 
    0x2, 0x2, 0x2d8, 0x2d4, 0x3, 0x2, 0x2, 0x2, 0x2d9, 0x63, 0x3, 0x2, 0x2, 
    0x2, 0x5f, 0x66, 0x68, 0x73, 0x7a, 0x7f, 0x85, 0x8b, 0x8d, 0x93, 0x9a, 
    0x9d, 0xa5, 0xaf, 0xb2, 0xb8, 0xba, 0xbe, 0xc5, 0xc9, 0xcc, 0xcf, 0xd8, 
    0xe3, 0xea, 0xed, 0xf5, 0xf9, 0x105, 0x10a, 0x10f, 0x11b, 0x123, 0x128, 
    0x131, 0x138, 0x13c, 0x14c, 0x14f, 0x157, 0x160, 0x164, 0x169, 0x179, 
    0x186, 0x19a, 0x1a0, 0x1a7, 0x1ab, 0x1b5, 0x1b8, 0x1be, 0x1c1, 0x1c3, 
    0x1c5, 0x1ce, 0x1d5, 0x1d8, 0x1dc, 0x1ec, 0x1f2, 0x1fa, 0x1ff, 0x202, 
    0x207, 0x210, 0x215, 0x223, 0x226, 0x228, 0x22d, 0x231, 0x234, 0x23b, 
    0x243, 0x247, 0x24b, 0x24e, 0x254, 0x257, 0x259, 0x262, 0x267, 0x26b, 
    0x26f, 0x272, 0x27d, 0x282, 0x289, 0x292, 0x297, 0x29a, 0x2a5, 0x2d8, 
  };

  atn::ATNDeserializer deserializer;
  _atn = deserializer.deserialize(_serializedATN);

  size_t count = _atn.getNumberOfDecisions();
  _decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    _decisionToDFA.emplace_back(_atn.getDecisionState(i), i);
  }
}

NormalSQLParser::Initializer NormalSQLParser::_init;
