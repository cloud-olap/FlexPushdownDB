//
// Created by matt on 29/4/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBE_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBE_H

#include <normal/core/Operator.h>
#include <normal/core/message/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>

#include "JoinPredicate.h"
#include "HashTableMessage.h"

namespace normal::pushdown::join {

/**
 * Operator implementing probe phase of a hash join
 *
 * Takes hashtable produced from build phase on one of the relations in the join (ideall the smaller) and uses it
 * to select rows from the both relations to include in the join.
 *
 */
class HashJoinProbe : public normal::core::Operator {

public:
  HashJoinProbe(const std::string &name, JoinPredicate pred);

  void onReceive(const core::message::Envelope &msg) override;

private:

  /**
   * The join predicate
   */
  JoinPredicate pred_;

  void onStart();
  void onTuple(core::message::TupleMessage msg);
  void onHashTable(HashTableMessage msg);
  void onComplete(core::message::CompleteMessage msg);

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBE_H
