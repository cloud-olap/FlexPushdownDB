//
// Created by Yifei Yang on 3/7/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_DISKMETRICSMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_DISKMETRICSMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/metrics/DiskMetrics.h>
#include <memory>

namespace fpdb::executor::message {

class DiskMetricsMessage : public Message {

public:
  DiskMetricsMessage(const executor::metrics::DiskMetrics &DiskMetrics,
                     const std::string &sender);
  DiskMetricsMessage() = default;
  DiskMetricsMessage(const DiskMetricsMessage&) = default;
  DiskMetricsMessage& operator=(const DiskMetricsMessage&) = default;
  ~DiskMetricsMessage() override = default;

  std::string getTypeString() const override;

  const executor::metrics::DiskMetrics &getDiskMetrics() const;

private:
  executor::metrics::DiskMetrics diskMetrics_;

  // caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, DiskMetricsMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("diskMetrics", msg.diskMetrics_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_DISKMETRICSMESSAGE_H
