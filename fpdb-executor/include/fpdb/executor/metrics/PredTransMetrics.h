//
// Created by Yifei Yang on 4/20/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_PREDTRANSMETRICS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_PREDTRANSMETRICS_H

#include <fpdb/tuple/serialization/ArrowSerializer.h>
#include <fpdb/util/Util.h>
#include <arrow/api.h>
#include <unordered_set>

namespace fpdb::executor::metrics {

class PredTransMetrics {

public:
  enum PTMetricsUnitType {
    LOCAL_FILTER,     // shows num rows after local filters
    BLOOM_FILTER,     // shows num rows after bloom filters
    PRED_TRANS,       // shows num rows after predicate transfer
  };

  static std::string PTMetricsUnitTypeToStr(PTMetricsUnitType type);

  struct PTMetricsInfo {
    bool collPredTransMetrics_ = false;
    uint prePOpId_;
    PTMetricsUnitType ptMetricsType_;
  };

  struct PTMetricsUnit {
    uint prePOpId_;
    std::string collectorPOpTypeStr_;
    PTMetricsUnitType type_;
    std::shared_ptr<arrow::Schema> schema_;
    mutable int64_t numRows_ = 0;

    PTMetricsUnit(uint prePOpId, const std::string &collectorPOpTypeStr, PTMetricsUnitType type,
                  const std::shared_ptr<arrow::Schema> &schema, int64_t numRows):
      prePOpId_(prePOpId), collectorPOpTypeStr_(collectorPOpTypeStr), type_(type),
      schema_(schema), numRows_(numRows) {}

    PTMetricsUnit() = default;
    PTMetricsUnit(const PTMetricsUnit&) = default;
    PTMetricsUnit& operator=(const PTMetricsUnit&) = default;
    ~PTMetricsUnit() = default;

    size_t hash() const {
      return fpdb::util::hashCombine({prePOpId_, std::hash<std::string>()(collectorPOpTypeStr_), type_});
    }

    bool equalTo(const PTMetricsUnit &other) const {
      return prePOpId_ == other.prePOpId_
          && collectorPOpTypeStr_ == other.collectorPOpTypeStr_
          && type_ == other.type_;
    }

    // caf inspect
    template <class Inspector>
    friend bool inspect(Inspector& f, PTMetricsUnit& unit) {
      auto schemaToBytes = [&unit]() -> decltype(auto) {
        return fpdb::tuple::ArrowSerializer::schema_to_bytes(unit.schema_);
      };
      auto schemaFromBytes = [&unit](const std::vector<std::uint8_t> &bytes) {
        unit.schema_ = fpdb::tuple::ArrowSerializer::bytes_to_schema(bytes);
        return true;
      };
      return f.object(unit).fields(f.field("prePOpId", unit.prePOpId_),
                                   f.field("collectorPOpTypeStr", unit.collectorPOpTypeStr_),
                                   f.field("type", unit.type_),
                                   f.field("schema", schemaToBytes, schemaFromBytes),
                                   f.field("numRows", unit.numRows_));
    }
  };

  struct PTMetricsUnitHash {
    inline size_t operator()(const PTMetricsUnit &unit) const {
      return unit.hash();
    }
  };

  struct PTMetricsUnitPred {
    inline bool operator()(const PTMetricsUnit &lhs, const PTMetricsUnit &rhs) const {
      return lhs.equalTo(rhs);
    }
  };

  PredTransMetrics() = default;
  PredTransMetrics(const PredTransMetrics&) = default;
  PredTransMetrics& operator=(const PredTransMetrics&) = default;
  ~PredTransMetrics() = default;

  const std::unordered_set<PTMetricsUnit, PTMetricsUnitHash, PTMetricsUnitPred> &getMetrics() const;
  void add(const PTMetricsUnit &unit);

private:
  std::unordered_set<PTMetricsUnit, PTMetricsUnitHash, PTMetricsUnitPred> metrics_;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_PREDTRANSMETRICS_H
