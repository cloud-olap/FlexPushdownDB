//
// Created by matt on 2/4/20.
//

#include <fpdb/plan/prephysical/AggregatePrePFunction.h>

namespace fpdb::plan::prephysical {

AggregatePrePFunction::AggregatePrePFunction(AggregatePrePFunctionType type,
                                             const shared_ptr<expression::gandiva::Expression> &expression) :
  type_(type),
  expression_(expression) {}

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