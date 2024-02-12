//
// Created by Yifei Yang on 11/22/21.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_UTIL_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_UTIL_H

#include <fpdb/expression/gandiva/Expression.h>

using namespace std;

namespace fpdb::expression::gandiva {

class Util {

public:
  static bool isLiteral(const shared_ptr<Expression> &expr);

};

}


#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_UTIL_H
