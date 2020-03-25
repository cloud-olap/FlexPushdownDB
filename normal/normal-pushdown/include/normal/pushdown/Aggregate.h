//
// Created by matt on 11/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_H
#define NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_H

#include <memory>
#include <string>
#include <vector>

#include <normal/core/Operator.h>
#include <normal/core/message/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/pushdown/aggregate/AggregationResult.h>
#include <normal/pushdown/aggregate/AggregationFunction.h>

namespace normal::pushdown {

class Aggregate : public normal::core::Operator {

private:
  std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> functions_;
  std::shared_ptr<aggregate::AggregationResult> result_;

  void onReceive(const normal::core::message::Envelope &message) override;

  void onTuple(const normal::core::message::TupleMessage &message);
  void onComplete(const normal::core::message::CompleteMessage &message);
  void onStart();

public:
  Aggregate(std::string name,
            std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> functions);
  ~Aggregate() override = default;

  void compute(const std::shared_ptr<normal::core::TupleSet> &tuples);

};

}

#endif //NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATE_H
