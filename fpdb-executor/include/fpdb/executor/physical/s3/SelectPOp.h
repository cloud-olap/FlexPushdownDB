//
// Created by matt on 21/3/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_SELECTPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_SELECTPOP_H

#include <aws/s3/model/InputSerialization.h>

#include "fpdb/executor/physical/s3/S3SelectScanAbstractPOp.h"


#ifdef __AVX2__
#include <fpdb/tuple/arrow/CSVToArrowSIMDChunkParser.h>
#endif

namespace fpdb::executor::physical::s3 {

class SelectPOp: public S3SelectScanAbstractPOp {
public:
  SelectPOp(std::string name,
              std::vector<std::string> projectColumnNames,
              int nodeId,
              std::string s3Bucket,
              std::string s3Object,
              std::string filterSql,
              int64_t startOffset,
              int64_t finishOffset,
              std::shared_ptr<Table> table,
              std::shared_ptr<AWSClient> awsClient,
              bool scanOnStart = true,
              bool toCache = false,
              std::vector<std::shared_ptr<fpdb::cache::SegmentKey>> weightedSegmentKeys = {});
  SelectPOp() = default;
  SelectPOp(const SelectPOp&) = default;
  SelectPOp& operator=(const SelectPOp&) = default;

  void clear() override;
  std::string getTypeString() const override;

private:
#ifdef __AVX2__
  std::shared_ptr<CSVToArrowSIMDChunkParser> generateSIMDCSVParser();
#endif
  std::shared_ptr<S3CSVParser> generateCSVParser();
  Aws::Vector<unsigned char> s3Result_{};

  // Scan range not supported in AWS for GZIP and BZIP2 CSV. We also don't support this for parquet yet either
  // as that is more complicated due to involving parquet metadata and we haven't had a chance to implement this yet
  bool scanRangeSupported();
  Aws::S3::Model::InputSerialization getInputSerialization();
  std::shared_ptr<TupleSet> s3Select(uint64_t startOffset, uint64_t endOffset);
  std::shared_ptr<TupleSet> flight_select();
  std::shared_ptr<TupleSet> s3SelectParallelReqs();
  // Wrapper function to encapsulate a thread spawned when making parallel requests
  void s3SelectIndividualReq(int reqNum, uint64_t startOffset, uint64_t endOffset);

  void processScanMessage(const ScanMessage &message) override;

  std::shared_ptr<TupleSet> readTuples() override;
  int getPredicateNum();
  void sendSegmentWeight();

  std::string filterSql_;   // "where ...."
  std::shared_ptr<S3CSVParser> parser_;

  /**
   * used to compute filter weight
   */
  std::vector<std::shared_ptr<fpdb::cache::SegmentKey>> weightedSegmentKeys_;

  // caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, SelectPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("consumerToBloomFilterInfo", op.consumerToBloomFilterInfo_),
                               f.field("isSeparated", op.isSeparated_),
                               f.field("s3Bucket", op.s3Bucket_),
                               f.field("s3Object", op.s3Object_),
                               f.field("filterSql", op.filterSql_),
                               f.field("startOffset", op.startOffset_),
                               f.field("finishOffset", op.finishOffset_),
                               f.field("table", op.table_),
                               f.field("scanOnStart", op.scanOnStart_),
                               f.field("toCache", op.toCache_),
                               f.field("weightedSegmentKeys", op.weightedSegmentKeys_));
  }
};

}


#endif // FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_SELECTPOP_H
