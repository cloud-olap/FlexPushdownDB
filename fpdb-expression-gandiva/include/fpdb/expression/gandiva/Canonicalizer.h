//
// Created by Yifei Yang on 11/22/21.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_CANONICALIZER_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_CANONICALIZER_H

#include <fpdb/expression/gandiva/Expression.h>

using namespace std;

namespace fpdb::expression::gandiva {

class Canonicalizer {

public:
  /**
   * Canonicalize expr so that for comparison, column is on the left and literal is on the right
   * @param expr
   * @return
   */
  static shared_ptr<Expression> canonicalize(const shared_ptr<Expression> &expr);

};

}


#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_CANONICALIZER_H
