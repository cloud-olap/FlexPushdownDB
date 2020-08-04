//
// Created by matt on 29/4/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBE_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBE_H

#include <normal/core/Operator.h>
#include <normal/pushdown/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>

#include "JoinPredicate.h"
#include "HashTableMessage.h"
#include "HashTable.h"
#include "HashJoinProbeKernel.h"
#include "HashJoinProbeKernel2.h"
#include "TupleSetIndexMessage.h"

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
   * A buffer of received tuples that are not joined until enough hashtable entries and tuples have been received
   */
//  std::shared_ptr<normal::tuple::TupleSet2> tuples_;

  /**
   * The hashtable
   */
//  std::shared_ptr<TupleSetIndex> hashtable_;

  HashJoinProbeKernel2 kernel_;

  void onStart();
  void onTuple(const core::message::TupleMessage &msg);
  void onHashTable(const TupleSetIndexMessage &msg);
  void onComplete(const core::message::CompleteMessage &msg);

  void bufferTuples(const core::message::TupleMessage &msg);
  void bufferHashTable(const TupleSetIndexMessage &msg);
  void joinAndSendTuples();
  tl::expected<std::shared_ptr<normal::tuple::TupleSet2>, std::string> join();
  void sendTuples(const std::shared_ptr<normal::tuple::TupleSet2> &tuples);

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBE_H
