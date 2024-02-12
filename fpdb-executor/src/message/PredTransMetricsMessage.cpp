//
// Created by Yifei Yang on 4/20/23.
//

#include <fpdb/executor/message/PredTransMetricsMessage.h>

namespace fpdb::executor::message {

PredTransMetricsMessage::PredTransMetricsMessage(const metrics::PredTransMetrics::PTMetricsUnit &ptMetrics,
                                                 const std::string &sender):
  Message(PRED_TRANS_METRICS, sender),
  ptMetrics_(ptMetrics) {}

std::string PredTransMetricsMessage::getTypeString() const {
  return "PredTransMetricsMessage";
}

const metrics::PredTransMetrics::PTMetricsUnit &PredTransMetricsMessage::getPTMetrics() const {
  return ptMetrics_;
}

}
