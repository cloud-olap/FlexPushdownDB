//
// Created by matt on 29/4/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_JOINPREDICATE_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_JOINPREDICATE_H

#include <string>

namespace normal::pushdown::join {

/**
 * A join predicate for straight column to column joins
 *
 * TODO: Support expressions
 */
class JoinPredicate {
  
public:
  JoinPredicate(std::string leftColumnName, std::string rightColumnName);
  const std::string &getLeftColumnName() const;
  const std::string &getRightColumnName() const;
  static JoinPredicate create(const std::string &leftColumnName, const std::string &rightColumnName);

private:
  std::string leftColumnName_;
  std::string rightColumnName_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_JOINPREDICATE_H
