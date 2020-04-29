//
// Created by matt on 29/4/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_JOINPREDICATE_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_JOINPREDICATE_H

#include <string>

namespace normal::pushdown::join {

class JoinPredicate {
  
public:
  JoinPredicate(const std::string &leftColumnName, const std::string &rightColumnName);
  
private:
  std::string leftField_;
  std::string rightField_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_JOINPREDICATE_H
