//
// Created by matt on 5/12/19.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_S3SELECTSCANABSTRACTPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_S3SELECTSCANABSTRACTPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/s3/S3SelectCSVParseOptions.h>
#include <fpdb/executor/physical/s3/S3CSVParser.h>
#include <fpdb/executor/message/TupleSetMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/ScanMessage.h>
#include <fpdb/executor/message/cache/LoadResponseMessage.h>
#include <fpdb/executor/message/cache/WeightRequestMessage.h>
#include <fpdb/catalogue/Table.h>
#include <fpdb/tuple/TupleSet.h>
#include <fpdb/aws/AWSClient.h>
#include <memory>
#include <string>

using namespace fpdb::executor::message;
using namespace fpdb::catalogue;
using namespace fpdb::aws;

namespace fpdb::executor::physical::s3 {

// Struct for passing around S3 GET/Select statistics
typedef struct S3SelectScanStats {
  size_t processedBytes; // bytes read in storage backend
  size_t returnedBytes;  // bytes returned from storage backend
  size_t outputBytes;    // bytes in the arrow result output from this operation
  size_t numRequests;    // number of requests sent to storage backend
  // For now we could combine the following into two variables as all current modes only do GET or Select
  // Leaving them separated so that we can easily support mixing GET/Select together in the future.
  size_t getTransferTimeNS;
  size_t getConvertTimeNS;
  size_t selectTransferTimeNS;
  size_t selectConvertTimeNS;
} S3SelectScanStats;

class S3SelectScanAbstractPOp : public PhysicalOp {
public:
  S3SelectScanAbstractPOp(std::string name,
         POpType type,
         std::vector<std::string> projectColumnNames,
         int nodeId,
			   std::string s3Bucket,
			   std::string s3Object,
			   int64_t startOffset,
			   int64_t finishOffset,
         std::shared_ptr<Table> table,
			   std::shared_ptr<AWSClient> awsClient,
			   bool scanOnStart,
			   bool toCache);
  S3SelectScanAbstractPOp();
  S3SelectScanAbstractPOp(const S3SelectScanAbstractPOp&) = default;
  S3SelectScanAbstractPOp& operator=(const S3SelectScanAbstractPOp&) = default;
  virtual ~S3SelectScanAbstractPOp() = default;

  void onReceive(const Envelope &message) override;

  S3SelectScanStats getS3SelectScanStats();

protected:
  void onStart();
  void onCacheLoadResponse(const ScanMessage &message);

  virtual void processScanMessage(const ScanMessage &message) = 0;
  virtual std::shared_ptr<TupleSet> readTuples() = 0;
  virtual void readAndSendTuples();

  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet> &tupleSet);
  void put(const std::shared_ptr<TupleSet> &tupleSet);

  std::string s3Bucket_;
  std::string s3Object_;
  uint64_t startOffset_;
  uint64_t finishOffset_;
  std::shared_ptr<Table> table_;
  std::shared_ptr<AWSClient> awsClient_;
  std::vector<std::shared_ptr<std::pair<std::string, ::arrow::ArrayVector>>> columnsReadFromS3_;

  S3SelectScanStats s3SelectScanStats_ = {0, 0, 0, 0 , 0, 0 ,0, 0};

  /**
   * Flags:
   * 1) scanOnStart_: send request to s3 at the beginning
   * 2) toCache_: whether the segments are to be cached
   */
  bool scanOnStart_;
  bool toCache_;

  // Used for collecting all results for split requests that are run in parallel, and for having a
  // locks on shared variables when requests are split.
  std::shared_ptr<std::mutex> splitReqLock_;
  std::map<int, std::shared_ptr<arrow::Table>> splitReqNumToTable_;
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_S3SELECTSCANABSTRACTPOP_H
