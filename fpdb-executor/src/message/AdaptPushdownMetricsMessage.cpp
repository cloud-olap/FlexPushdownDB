//
// Created by Yifei Yang on 10/31/22.
//

#include <fpdb/executor/message/AdaptPushdownMetricsMessage.h>
#include <fpdb/executor/physical/Globals.h>
#include <fmt/format.h>
#include <fpdb/util/Util.h>

using namespace fpdb::executor::physical;

namespace fpdb::executor::message {

AdaptPushdownMetricsMessage::AdaptPushdownMetricsMessage(const std::string &key,
                                                         int64_t execTime,
                                                         const std::string &sender):
  Message(ADAPT_PUSHDOWN_METRICS, sender),
  key_(key),
  execTime_(execTime) {}

tl::expected<std::string, std::string>
AdaptPushdownMetricsMessage::generateAdaptPushdownMetricsKey(long queryId, const std::string &op) {
  if (op.substr(0, PullupOpNamePrefix.size()) == PullupOpNamePrefix) {
    // pullup metrics key, e.g.
    // (0, RemoteFileScan[7]-flexpushdowndb/tpch-sf0.01/parquet/lineitem_sharded/lineitem.parquet.0) -> 0-RemoteFileScan[6]-0
    auto truncateStartPos = op.find("]");
    auto truncateEndPos = op.find_last_of('.') + 1;
    auto partitionIdStr = op.substr(truncateEndPos, op.length() - truncateEndPos);
    std::string a;
    if (fpdb::util::isInteger(partitionIdStr)) {
      return fmt::format("{}-{}-{}", queryId, op.substr(0, truncateStartPos + 1), partitionIdStr);
    } else {
      return fmt::format("{}-{}-0", queryId, op.substr(0, truncateStartPos + 1));
    }
  } else if (op.substr(0, PushdownOpNamePrefix.size()) == PushdownOpNamePrefix) {
    // pushdown metrics key, e.g. (0, FPDBStoreSuper[6]-0) -> 0-FPDBStoreSuper[6]-0
    return fmt::format("{}-{}", queryId, op);
  } else {
    return tl::make_unexpected(fmt::format("Invalid op name for adaptive pushdown metrics: {}", op));
  }
}

std::string AdaptPushdownMetricsMessage::getTypeString() const {
  return "AdaptPushdownMetricsMessage";
}

const std::string &AdaptPushdownMetricsMessage::getKey() const {
  return key_;
}

int64_t AdaptPushdownMetricsMessage::getExecTime() const {
  return execTime_;
}

}
