
// Generated from /Users/yyf/Desktop/pushdownDB/normal/normal-sql/grammar/NormalSQL.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"


namespace normal::sql {


class  NormalSQLLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, SCOL = 2, DOT = 3, OPEN_PAR = 4, CLOSE_PAR = 5, COMMA = 6, 
    ASSIGN = 7, STAR = 8, PLUS = 9, MINUS = 10, TILDE = 11, PIPE2 = 12, 
    DIV = 13, MOD = 14, LT2 = 15, GT2 = 16, AMP = 17, PIPE = 18, LT = 19, 
    LT_EQ = 20, GT = 21, GT_EQ = 22, EQ = 23, NOT_EQ1 = 24, NOT_EQ2 = 25, 
    K_ABORT = 26, K_ACTION = 27, K_ADD = 28, K_AFTER = 29, K_ALL = 30, K_ALTER = 31, 
    K_ANALYZE = 32, K_AND = 33, K_AS = 34, K_ASC = 35, K_ATTACH = 36, K_AUTOINCREMENT = 37, 
    K_BEFORE = 38, K_BEGIN = 39, K_BETWEEN = 40, K_BY = 41, K_CASCADE = 42, 
    K_CASE = 43, K_CAST = 44, K_CHECK = 45, K_COLLATE = 46, K_COLUMN = 47, 
    K_COMMIT = 48, K_CONFLICT = 49, K_CONSTRAINT = 50, K_CREATE = 51, K_CROSS = 52, 
    K_CURRENT_DATE = 53, K_CURRENT_TIME = 54, K_CURRENT_TIMESTAMP = 55, 
    K_DATABASE = 56, K_DEFAULT = 57, K_DEFERRABLE = 58, K_DEFERRED = 59, 
    K_DELETE = 60, K_DESC = 61, K_DETACH = 62, K_DISTINCT = 63, K_DROP = 64, 
    K_EACH = 65, K_ELSE = 66, K_END = 67, K_ESCAPE = 68, K_EXCEPT = 69, 
    K_EXCLUSIVE = 70, K_EXISTS = 71, K_EXPLAIN = 72, K_FAIL = 73, K_FOR = 74, 
    K_FOREIGN = 75, K_FROM = 76, K_FULL = 77, K_GLOB = 78, K_GROUP = 79, 
    K_HAVING = 80, K_IF = 81, K_IGNORE = 82, K_IMMEDIATE = 83, K_IN = 84, 
    K_INDEX = 85, K_INDEXED = 86, K_INITIALLY = 87, K_INNER = 88, K_INSERT = 89, 
    K_INSTEAD = 90, K_INTERSECT = 91, K_INTO = 92, K_IS = 93, K_ISNULL = 94, 
    K_JOIN = 95, K_KEY = 96, K_LEFT = 97, K_LIKE = 98, K_LIMIT = 99, K_MATCH = 100, 
    K_NATURAL = 101, K_NO = 102, K_NOT = 103, K_NOTNULL = 104, K_NULL = 105, 
    K_OF = 106, K_OFFSET = 107, K_ON = 108, K_OR = 109, K_ORDER = 110, K_OUTER = 111, 
    K_PLAN = 112, K_PRAGMA = 113, K_PRIMARY = 114, K_QUERY = 115, K_RAISE = 116, 
    K_RECURSIVE = 117, K_REFERENCES = 118, K_REGEXP = 119, K_REINDEX = 120, 
    K_RELEASE = 121, K_RENAME = 122, K_REPLACE = 123, K_RESTRICT = 124, 
    K_RIGHT = 125, K_ROLLBACK = 126, K_ROW = 127, K_SAVEPOINT = 128, K_SELECT = 129, 
    K_SET = 130, K_TABLE = 131, K_TEMP = 132, K_TEMPORARY = 133, K_THEN = 134, 
    K_TO = 135, K_TRANSACTION = 136, K_TRIGGER = 137, K_UNION = 138, K_UNIQUE = 139, 
    K_UPDATE = 140, K_USING = 141, K_VACUUM = 142, K_VALUES = 143, K_VIEW = 144, 
    K_VIRTUAL = 145, K_WHEN = 146, K_WHERE = 147, K_WITH = 148, K_WITHOUT = 149, 
    IDENTIFIER = 150, NUMERIC_LITERAL = 151, BIND_PARAMETER = 152, STRING_LITERAL = 153, 
    BLOB_LITERAL = 154, SINGLE_LINE_COMMENT = 155, MULTILINE_COMMENT = 156, 
    SPACES = 157, UNEXPECTED_CHAR = 158
  };

  NormalSQLLexer(antlr4::CharStream *input);
  ~NormalSQLLexer();

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames() const override; // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

}  // namespace normal::sql
