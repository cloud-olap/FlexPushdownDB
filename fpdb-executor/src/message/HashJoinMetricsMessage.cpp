//
// Created by Yifei Yang on 2/26/24.
//

#include <fpdb/executor/message/HashJoinMetricsMessage.h>

namespace fpdb::executor::message {

HashJoinMetricsMessage::HashJoinMetricsMessage(const executor::metrics::HashJoinMetrics &hashJoinMetrics,
                                               const std::string &sender):
  Message(HASH_JOIN_METRICS, sender),
  hashJoinMetrics_(hashJoinMetrics) {}

std::string HashJoinMetricsMessage::getTypeString() const {
  return "HashJoinMetricsMessage";
}

const executor::metrics::HashJoinMetrics &HashJoinMetricsMessage::getHashJoinMetrics() const {
  return hashJoinMetrics_;
}

}
