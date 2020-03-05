//
// Created by matt on 11/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_H
#define NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_H

#include <memory>                                 // for unique_ptr
#include <string>                                 // for string
#include <vector>                                 // for vector

#include <normal/core/Operator.h>
#include <normal/core/TupleMessage.h>
#include <normal/core/CompleteMessage.h>

#include "AggregateExpression.h"

namespace normal::pushdown {

class Aggregate : public normal::core::Operator {

private:
  std::vector<std::unique_ptr<AggregateExpression>> expressions_;
  std::shared_ptr<normal::core::TupleSet> inputTuples;
  std::shared_ptr<normal::core::TupleSet> outputTuples;

  void onReceive(const normal::core::Envelope &message) override;
  void onTuple(normal::core::TupleMessage message);
  void onComplete(const normal::core::CompleteMessage &message);
  void onStart();

public:
  Aggregate(std::string name, std::vector<std::unique_ptr<AggregateExpression>> expressions);
  ~Aggregate() override = default;

};

}

#endif //NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_H
