//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H
#define NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H

#include <memory>
#include <string>

#include <aws/s3/S3Client.h>

#include <normal/core/message/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include <normal/pushdown/scan/ScanMessage.h>
#include <normal/core/cache/WeightRequestMessage.h>

#include "normal/core/Operator.h"
#include "normal/tuple/TupleSet.h"
#include "normal/pushdown/s3/S3SelectCSVParseOptions.h"
#include "normal/pushdown/s3/S3CSVParser.h"

using namespace normal::core;
using namespace normal::core::message;
using namespace normal::core::cache;

namespace normal::pushdown::s3 {

// Struct for passing around S3 GET/Select statistics
typedef struct S3SelectScanStats {
  size_t processedBytes; // bytes read in storage backend
  size_t returnedBytes;  // bytes returned from storage backen
  size_t outputBytes;    // bytes in the arrow result output from this operation
  size_t numRequests;    // number of requests sent to storage backend
  // For now we could combine the following into two variables as all current modes only do GET or Select
  // Leaving them separated so that we can easily support mixing GET/Select together in the future.
  size_t getTransferTimeNS;
  size_t getConvertTimeNS;
  size_t selectTransferTimeNS;
  size_t selectConvertTimeNS;
} S3SelectScanStats;

class S3SelectScan : public normal::core::Operator {
public:
  S3SelectScan(std::string name,
			   std::string type,
			   std::string s3Bucket,
			   std::string s3Object,
			   std::vector<std::string> returnedS3ColumnNames,
			   std::vector<std::string> neededColumnNames,
			   int64_t startOffset,
			   int64_t finishOffset,
         std::shared_ptr<arrow::Schema> schema,
			   std::shared_ptr<Aws::S3::S3Client> s3Client,
			   bool scanOnStart,
			   bool toCache,
			   long queryId,
         std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> weightedSegmentKeys);

  S3SelectScanStats getS3SelectScanStats();

protected:
  std::string s3Bucket_;
  std::string s3Object_;
  std::vector<std::string> returnedS3ColumnNames_;
	std::vector<std::string> neededColumnNames_;
  uint64_t startOffset_;
  uint64_t finishOffset_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<Aws::S3::S3Client> s3Client_;
  std::vector<std::shared_ptr<std::pair<std::string, ::arrow::ArrayVector>>> columnsReadFromS3_;

  S3SelectScanStats s3SelectScanStats_ = {0, 0, 0, 0 , 0, 0 ,0, 0};


  /**
   * Flags:
   * 1) scanOnStart_: send request to s3 at the beginning
   * 2) toCache_: whether the segments are to be cached
   */
  bool scanOnStart_;
  bool toCache_;

  void onStart();
  void onReceive(const Envelope &message) override;
  void onCacheLoadResponse(const scan::ScanMessage &message);
  virtual void processScanMessage(const scan::ScanMessage &message) = 0;

  void put(const std::shared_ptr<TupleSet2> &tupleSet);

  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet);
  virtual std::shared_ptr<TupleSet2> readTuples() = 0;
  virtual int getPredicateNum() = 0;
  virtual void readAndSendTuples();
  void sendSegmentWeight();

  /**
   * used to compute filter weight
   */
  std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> weightedSegmentKeys_;
};

}

#endif //NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H
