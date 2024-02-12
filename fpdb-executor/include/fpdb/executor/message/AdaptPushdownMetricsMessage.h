//
// Created by Yifei Yang on 10/31/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_ADAPTPUSHDOWNMETRICSMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_ADAPTPUSHDOWNMETRICSMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <tl/expected.hpp>

namespace fpdb::executor::message {

class AdaptPushdownMetricsMessage : public Message {

public:
  AdaptPushdownMetricsMessage(const std::string &key,
                              int64_t execTime,
                              const std::string &sender);
  AdaptPushdownMetricsMessage() = default;
  AdaptPushdownMetricsMessage(const AdaptPushdownMetricsMessage&) = default;
  AdaptPushdownMetricsMessage& operator=(const AdaptPushdownMetricsMessage&) = default;
  ~AdaptPushdownMetricsMessage() override = default;

  static tl::expected<std::string, std::string> generateAdaptPushdownMetricsKey(long queryId, const std::string &op);

  std::string getTypeString() const override;

  const std::string &getKey() const;
  int64_t getExecTime() const;

private:
  std::string key_;
  int64_t execTime_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, AdaptPushdownMetricsMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("key", msg.key_),
                                f.field("execTime", msg.execTime_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_ADAPTPUSHDOWNMETRICSMESSAGE_H
