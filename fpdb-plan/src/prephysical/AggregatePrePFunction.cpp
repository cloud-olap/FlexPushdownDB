//
// Created by matt on 2/4/20.
//

#include <fpdb/plan/prephysical/AggregatePrePFunction.h>

namespace fpdb::plan::prephysical {

AggregatePrePFunction::AggregatePrePFunction(AggregatePrePFunctionType type,
                                             const shared_ptr<expression::gandiva::Expression> &expression) :
  type_(type),
  expression_(expression) {}

bool AggregatePrePFunction::equals(const std::shared_ptr<AggregatePrePFunction> &f1,
                                   const std::shared_ptr<AggregatePrePFunction> &f2) {
  if (f1 == nullptr && f2 == nullptr) {
    return true;
  } else if (f1 != nullptr && f2 != nullptr) {
    return f1->type_ == f2->type_ && expression::gandiva::Expression::equals(f1->expression_, f2->expression_);
  } else {
    return false;
  }
}

bool AggregatePrePFunction::equals(const std::vector<std::shared_ptr<AggregatePrePFunction>> &f1,
                                   const std::vector<std::shared_ptr<AggregatePrePFunction>> &f2) {
  if (f1.size() != f2.size()) {
    return false;
  }
  for (uint i = 0; i < f1.size(); ++i) {
    if (!equals(f1[i], f2[i])) {
      return false;
    }
  }
  return true;
}

AggregatePrePFunctionType AggregatePrePFunction::getType() const {
  return type_;
}

const shared_ptr<expression::gandiva::Expression> &AggregatePrePFunction::getExpression() const {
  return expression_;
}

string AggregatePrePFunction::getTypeString() const {
  switch (type_) {
    case SUM: return "SUM";
    case COUNT: return "COUNT";
    case MAX: return "MAX";
    case MIN: return "MIN";
    case AVG: return "AVG";
    default: return "UNKNOWN";
  }
}

set<string> AggregatePrePFunction::involvedColumnNames() const {
  if (expression_) {
    return expression_->involvedColumnNames();
  } else {
    if (type_ == COUNT) {
      // count(*)
      return set<string>({COUNT_STAR_COLUMN});
    } else {
      return set<string>();
    }
  }
}

}