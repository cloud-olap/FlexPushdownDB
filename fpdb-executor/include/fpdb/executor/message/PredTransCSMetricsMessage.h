//
// Created by Yifei Yang on 1/10/25.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_PREDTRANSCSMETRICSMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_PREDTRANSCSMETRICSMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/metrics/PredTransMetrics.h>
#include <memory>

namespace fpdb::executor::message {

class PredTransCSMetricsMessage : public Message {
public:
  PredTransCSMetricsMessage(const metrics::PredTransCSMetrics::PTCSMetricsUnit &ptCSMetrics,
                            const std::string &sender);
  PredTransCSMetricsMessage() = default;
  PredTransCSMetricsMessage(const PredTransCSMetricsMessage&) = default;
  PredTransCSMetricsMessage& operator=(const PredTransCSMetricsMessage&) = default;
  ~PredTransCSMetricsMessage() override = default;

  std::string getTypeString() const override;

  const metrics::PredTransCSMetrics::PTCSMetricsUnit &getPTCSMetrics() const;

private:
  metrics::PredTransCSMetrics::PTCSMetricsUnit ptCSMetrics_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, PredTransCSMetricsMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("ptCSMetrics", msg.ptCSMetrics_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_PREDTRANSCSMETRICSMESSAGE_H
