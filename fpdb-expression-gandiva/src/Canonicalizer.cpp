//
// Created by Yifei Yang on 11/22/21.
//

#include <fpdb/expression/gandiva/Canonicalizer.h>
#include <fpdb/expression/gandiva/And.h>
#include <fpdb/expression/gandiva/Or.h>
#include <fpdb/expression/gandiva/EqualTo.h>
#include <fpdb/expression/gandiva/LessThan.h>
#include <fpdb/expression/gandiva/LessThanOrEqualTo.h>
#include <fpdb/expression/gandiva/GreaterThan.h>
#include <fpdb/expression/gandiva/GreaterThanOrEqualTo.h>
#include <fpdb/expression/gandiva/Util.h>

namespace fpdb::expression::gandiva {

shared_ptr<Expression> Canonicalizer::canonicalize(const shared_ptr<Expression> &expr) {
  auto type = expr->getType();
  switch (type) {
    case AND: {
      const auto &andExpr = static_pointer_cast<And>(expr);
      vector<shared_ptr<Expression>> canonicalizedChildExprs;
      for (const auto &childExpr: andExpr->getExprs()) {
        canonicalizedChildExprs.emplace_back(canonicalize(childExpr));
      }
      return and_(canonicalizedChildExprs);
    }

    case OR: {
      const auto &orExpr = static_pointer_cast<Or>(expr);
      vector<shared_ptr<Expression>> canonicalizedChildExprs;
      for (const auto &childExpr: orExpr->getExprs()) {
        canonicalizedChildExprs.emplace_back(canonicalize(childExpr));
      }
      return or_(canonicalizedChildExprs);
    }

    case EQUAL_TO:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL_TO:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL_TO: {
      const auto &biExpr = static_pointer_cast<BinaryExpression>(expr);
      const auto &leftExpr = biExpr->getLeft();
      const auto &rightExpr = biExpr->getRight();

      if (Util::isLiteral(leftExpr) && !Util::isLiteral(rightExpr)) {
        switch (type) {
          case EQUAL_TO: return eq(rightExpr, leftExpr);
          case LESS_THAN: return gt(rightExpr, leftExpr);
          case LESS_THAN_OR_EQUAL_TO: return gte(rightExpr, leftExpr);
          case GREATER_THAN: return lt(rightExpr, leftExpr);
          default: return lte(rightExpr, leftExpr);
        }
      }
    }

    default:
      return expr;
  };
}

}
