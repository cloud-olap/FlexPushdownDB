//
// Created by Yifei Yang on 1/10/25.
//

#include <fpdb/executor/message/PredTransCSMetricsMessage.h>

namespace fpdb::executor::message {

PredTransCSMetricsMessage::PredTransCSMetricsMessage(const metrics::PredTransCSMetrics::PTCSMetricsUnit &ptCSMetrics,
                                                     const std::string &sender):
  Message(PRED_TRANS_CS_METRICS, sender),
  ptCSMetrics_(ptCSMetrics) {}

std::string PredTransCSMetricsMessage::getTypeString() const {
  return "PredTransCSMetricsMessage";
}

const metrics::PredTransCSMetrics::PTCSMetricsUnit &PredTransCSMetricsMessage::getPTCSMetrics() const {
  return ptCSMetrics_;
}

}
