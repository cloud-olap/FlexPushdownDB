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

  enum PTPhaseType {
    PRED_TRANS_PHASE,     // predicate transfer phase (BF / semi-join)
    JOIN_PHASE,           // join phase
    OTHER                 // for ops neither in pred-trans phase nor in join phase
  };

  static std::string PTMetricsUnitTypeToStr(PTMetricsUnitType type);

  struct PTMetricsInfo {
    bool collPredTransMetrics_ = false;
    uint prePOpId_;               // from which PrePhysicalOp we record the size after pre-filtering
                                  // this is the identifier of the metrics
    std::string table_;           // for which base table we record the size after pre-filtering
                                  // it may be derived table (e.g., group result) instead of base table
    PTMetricsUnitType ptMetricsType_;

    // caf inspect
    template <class Inspector>
    friend bool inspect(Inspector& f, PTMetricsInfo& info) {
      return f.object(info).fields(f.field("collPredTransMetrics_", info.collPredTransMetrics_),
                                   f.field("prePOpId", info.prePOpId_),
                                   f.field("table", info.table_),
                                   f.field("ptMetricsType", info.ptMetricsType_));
    }
  };

  struct PTMetricsUnit {
    uint prePOpId_;
    std::string table_;
    std::string collectorPOpTypeStr_;
    PTMetricsUnitType type_;
    std::shared_ptr<arrow::Schema> schema_;
    mutable int64_t numRows_ = 0;

    PTMetricsUnit(uint prePOpId, std::string table,
                  const std::string &collectorPOpTypeStr, PTMetricsUnitType type,
                  const std::shared_ptr<arrow::Schema> &schema, int64_t numRows):
      prePOpId_(prePOpId), table_(table),
      collectorPOpTypeStr_(collectorPOpTypeStr), type_(type),
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
                                   f.field("table", unit.table_),
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

/**
 * Currnetly only collect for `SINGLE-NODE`, `BCAST-BF`, `PTION-SRC-BF-DST-VAL`, and
 * `PTION-SRC-VAL-DST-BF` PT.
 */
class PredTransCSMetrics {
public:
  enum PTCSMetricsBfTimeType {
    BUILD,     // op time should be recorded as BF build time
    PROBE,     // op time should be recorded as BF probe time
    UNKNOWN,
  };

  struct PTCSMetricsInfo {
    bool collPredTransCSMetrics_ = false;
    bool forward_;
    uint step_;
    uint srcPrePOpId_, dstPrePOpId_;
    std::string srcTable_, dstTable_;
    std::string distPTType_;
    PTCSMetricsBfTimeType bfTimeType_;
    bool collFiltering_ = true;   // Whether for `bfTimeType_ == PROBE`, we collect for `numRowsIn_` and
                                  // `numRowsOut_`, since for ption-based dist PT we should only collect
                                  // for `post probe`.

    PTCSMetricsInfo(bool forward, uint step,
                    uint srcPrePOpId, uint dstPrePOpId,
                    const std::string &srcTable, const std::string &dstTable,
                    const std::string &distPTType,
                    PTCSMetricsBfTimeType bfTimeType):
      collPredTransCSMetrics_(true),
      forward_(forward), step_(step),
      srcPrePOpId_(srcPrePOpId), dstPrePOpId_(dstPrePOpId),
      srcTable_(srcTable), dstTable_(dstTable),
      distPTType_(distPTType),
      bfTimeType_(bfTimeType) {}
    
    PTCSMetricsInfo() = default;
    PTCSMetricsInfo(const PTCSMetricsInfo&) = default;
    PTCSMetricsInfo& operator=(const PTCSMetricsInfo&) = default;
    ~PTCSMetricsInfo() = default;

    // caf inspect
    template <class Inspector>
    friend bool inspect(Inspector& f, PTCSMetricsInfo& info) {
      return f.object(info).fields(f.field("collPredTransCSMetrics", info.collPredTransCSMetrics_),
                                   f.field("forward", info.forward_),
                                   f.field("step", info.step_),
                                   f.field("srcPrePOpId", info.srcPrePOpId_),
                                   f.field("dstPrePOpId", info.dstPrePOpId_),
                                   f.field("srcTable", info.srcTable_),
                                   f.field("dstTable", info.dstTable_),
                                   f.field("distPTType", info.distPTType_),
                                   f.field("bfTimeType", info.bfTimeType_),
                                   f.field("collFiltering", info.collFiltering_));
    }
  };

  struct PTCSMetricsUnit {
    PTCSMetricsInfo info_;
    mutable int64_t numRowsIn_ = 0, numRowsOut_ = 0;
    mutable int64_t bfSize_ = 0;                          // in bytes
    mutable int64_t bfBuildTime_ = 0, bfProbeTime_ = 0;   // in ns, including network time

    PTCSMetricsUnit(const PTCSMetricsInfo &info):
      info_(info) {}

    PTCSMetricsUnit() = default;
    PTCSMetricsUnit(const PTCSMetricsUnit&) = default;
    PTCSMetricsUnit& operator=(const PTCSMetricsUnit&) = default;
    ~PTCSMetricsUnit() = default;

    size_t hash() const {
      return fpdb::util::hashCombine({info_.forward_, info_.step_});
    }

    bool equalTo(const PTCSMetricsUnit &other) const {
      return info_.forward_ == other.info_.forward_
          && info_.step_ == other.info_.step_;
    }

    // caf inspect
    template <class Inspector>
    friend bool inspect(Inspector& f, PTCSMetricsUnit& unit) {
      return f.object(unit).fields(f.field("info", unit.info_),
                                   f.field("numRowsIn", unit.numRowsIn_),
                                   f.field("numRowsOut", unit.numRowsOut_),
                                   f.field("bfSize", unit.bfSize_),
                                   f.field("bfBuildTime", unit.bfBuildTime_),
                                   f.field("bfProbeTime", unit.bfProbeTime_));
    }
  };

  struct PTCSMetricsUnitHash {
    inline size_t operator()(const PTCSMetricsUnit &unit) const {
      return unit.hash();
    }
  };

  struct PTCSMetricsUnitPred {
    inline bool operator()(const PTCSMetricsUnit &lhs, const PTCSMetricsUnit &rhs) const {
      return lhs.equalTo(rhs);
    }
  };

  const std::unordered_set<PTCSMetricsUnit, PTCSMetricsUnitHash, PTCSMetricsUnitPred> &getMetrics() const;
  void add(const PTCSMetricsUnit &unit);

private:
  std::unordered_set<PTCSMetricsUnit, PTCSMetricsUnitHash, PTCSMetricsUnitPred> metrics_;
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_METRICS_PREDTRANSMETRICS_H
