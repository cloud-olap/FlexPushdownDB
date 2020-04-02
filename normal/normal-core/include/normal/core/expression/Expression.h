//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_EXPRESSION_H
#define NORMAL_NORMAL_CORE_SRC_EXPRESSION_H

#include <string>

template<class T>
class Expression {
public:
  virtual T eval() = 0;

};

#endif //NORMAL_NORMAL_CORE_SRC_EXPRESSION_H
