//
// Created by Yifei Yang on 2/26/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_HASHJOINMETRICSMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_HASHJOINMETRICSMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/metrics/HashJoinMetrics.h>
#include <memory>

namespace fpdb::executor::message {

class HashJoinMetricsMessage: public Message {

public:
  HashJoinMetricsMessage(const executor::metrics::HashJoinMetrics &hashJoinMetrics,
                         const std::string &sender);
  HashJoinMetricsMessage() = default;
  HashJoinMetricsMessage(const HashJoinMetricsMessage&) = default;
  HashJoinMetricsMessage& operator=(const HashJoinMetricsMessage&) = default;
  ~HashJoinMetricsMessage() override = default;

  std::string getTypeString() const override;

  const executor::metrics::HashJoinMetrics &getHashJoinMetrics() const;

private:
  executor::metrics::HashJoinMetrics hashJoinMetrics_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, HashJoinMetricsMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("hashJoinMetrics", msg.hashJoinMetrics_));
  }

};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_HASHJOINMETRICSMESSAGE_H
