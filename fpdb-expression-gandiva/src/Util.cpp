//
// Created by Yifei Yang on 11/22/21.
//

#include <fpdb/expression/gandiva/Util.h>
#include <fpdb/expression/gandiva/Cast.h>

namespace fpdb::expression::gandiva {

bool Util::isLiteral(const shared_ptr <Expression> &expr) {
  return expr->getType() == NUMERIC_LITERAL || expr->getType() == STRING_LITERAL;
}

}
