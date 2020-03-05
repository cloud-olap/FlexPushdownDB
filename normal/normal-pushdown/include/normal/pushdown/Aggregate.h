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

class Aggregate : public normal::core::Operator {
private:
  std::vector<std::unique_ptr<AggregateExpression>> m_expressions;
  std::shared_ptr<normal::core::TupleSet> inputTupleSet;
  std::shared_ptr<normal::core::TupleSet> resultTupleSet;
public:
  Aggregate(std::string name, std::vector<std::unique_ptr<AggregateExpression>> m_expressions);
  ~Aggregate() override = default;
  void onStart();
  void onReceive(const normal::core::Envelope& msg) override;
protected:
  void onTuple(normal::core::TupleMessage Message);
  void onComplete(const normal::core::CompleteMessage &msg);
};

#endif //NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_H
