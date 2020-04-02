//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_EXPRESSION_H
#define NORMAL_NORMAL_CORE_SRC_EXPRESSION_H

namespace normal::core::expression {

template<typename T>
class Expression {
public:
  virtual ~Expression() = default;

  virtual T eval() = 0;

};

}

#endif //NORMAL_NORMAL_CORE_SRC_EXPRESSION_H
