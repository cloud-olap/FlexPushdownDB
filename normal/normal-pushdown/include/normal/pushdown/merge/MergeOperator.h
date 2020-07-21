//
// Created by matt on 20/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_MERGE_MERGEOPERATOR_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_MERGE_MERGEOPERATOR_H

#include <normal/core/Operator.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/pushdown/TupleMessage.h>
#include <queue>

using namespace normal::core;
using namespace normal::core::message;

namespace normal::pushdown::merge {

class MergeOperator : public Operator {

public:

  explicit MergeOperator(const std::string &Name);

  static std::shared_ptr<MergeOperator> make(const std::string &Name);

  void onReceive(const Envelope &msg) override;

  void onStart();
  void onComplete(const CompleteMessage &);
  void onTuple(const TupleMessage &message);

private:

  std::optional<LocalOperatorDirectoryEntry> leftProducer_  = std::nullopt;
  std::optional<LocalOperatorDirectoryEntry> rightProducer_  = std::nullopt;

  std::list<std::shared_ptr<TupleSet2>> leftTupleSets_;
  std::list<std::shared_ptr<TupleSet2>> rightTupleSets_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_MERGE_MERGEOPERATOR_H
