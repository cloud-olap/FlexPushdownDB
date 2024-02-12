//
// Created by Yifei Yang on 4/20/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_PREDTRANSMETRICSMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_PREDTRANSMETRICSMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/metrics/PredTransMetrics.h>
#include <memory>

namespace fpdb::executor::message {

class PredTransMetricsMessage : public Message {

public:
  PredTransMetricsMessage(const metrics::PredTransMetrics::PTMetricsUnit &ptMetrics,
                          const std::string &sender);
  PredTransMetricsMessage() = default;
  PredTransMetricsMessage(const PredTransMetricsMessage&) = default;
  PredTransMetricsMessage& operator=(const PredTransMetricsMessage&) = default;
  ~PredTransMetricsMessage() override = default;

  std::string getTypeString() const override;

  const metrics::PredTransMetrics::PTMetricsUnit &getPTMetrics() const;

private:
  metrics::PredTransMetrics::PTMetricsUnit ptMetrics_;

  // caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, PredTransMetricsMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("ptMetrics", msg.ptMetrics_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_PREDTRANSMETRICSMESSAGE_H
