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

class Merge : public Operator {

public:

  explicit Merge(const std::string &Name);

  static std::shared_ptr<Merge> make(const std::string &Name);

  void onReceive(const Envelope &msg) override;

  void onStart();
  void onComplete(const CompleteMessage &);
  void onTuple(const TupleMessage &message);

  void setLeftProducer(const std::shared_ptr<Operator> &leftProducer);
  void setRightProducer(const std::shared_ptr<Operator> &rightProducer);

private:

  void merge();

  std::shared_ptr<Operator> leftProducer_;
  std::shared_ptr<Operator> rightProducer_;

  std::list<std::shared_ptr<TupleSet2>> leftTupleSets_;
  std::list<std::shared_ptr<TupleSet2>> rightTupleSets_;

  // Flags to make sure CompleteMessage is sent after all TupleMessages have been sent
  int onTupleNum_ = 0;
  bool tupleArrived_ = false;
  std::mutex mergeLock;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_MERGE_MERGEOPERATOR_H
