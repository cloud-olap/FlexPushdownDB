//
// Created by Yifei Yang on 4/20/23.
//

#include <fpdb/executor/metrics/PredTransMetrics.h>
#include <fmt/format.h>

namespace fpdb::executor::metrics {

std::string PredTransMetrics::PTMetricsUnitTypeToStr(PTMetricsUnitType type) {
  switch (type) {
    case PTMetricsUnitType::LOCAL_FILTER: {
      return "Local Filter";
    }
    case PTMetricsUnitType::BLOOM_FILTER: {
      return "Bloom Filter";
    }
    case PTMetricsUnitType::PRED_TRANS: {
      return "Predicate Transfer";
    }
    default: {
      throw std::runtime_error(fmt::format("Unknown PTMetricsUnitType: '{}'", type));
    }
  }
}

const std::unordered_set<PredTransMetrics::PTMetricsUnit,
                         PredTransMetrics::PTMetricsUnitHash,
                         PredTransMetrics::PTMetricsUnitPred> &PredTransMetrics::getMetrics() const {
  return metrics_;
}

void PredTransMetrics::add(const PTMetricsUnit &unit) {
  auto it = metrics_.find(unit);
  if (it == metrics_.end()) {
    metrics_.emplace(unit);
  } else {
    it->numRows_ += unit.numRows_;
  }
}

}
