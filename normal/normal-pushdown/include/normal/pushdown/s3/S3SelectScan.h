//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H
#define NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H

#include <memory>
#include <string>

#include <aws/s3/S3Client.h>

#include <normal/pushdown/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include <normal/pushdown/scan/ScanMessage.h>
#include <normal/core/cache/WeightRequestMessage.h>

#include "normal/core/Operator.h"
#include "normal/tuple/TupleSet.h"
#include "normal/pushdown/s3/S3SelectCSVParseOptions.h"
#include "normal/pushdown/s3/S3SelectParser.h"

using namespace normal::core;
using namespace normal::core::message;
using namespace normal::core::cache;

namespace normal::pushdown {

class S3SelectScan : public normal::core::Operator {

  typedef std::function<void(const std::shared_ptr<TupleSet2> &)> TupleSetEventCallback;

public:
  S3SelectScan(std::string name,
			   std::string s3Bucket,
			   std::string s3Object,
			   std::string filterSql,
			   std::string tableName,
			   std::vector<std::string> columnNames,
			   int64_t startOffset,
			   int64_t finishOffset,
			   S3SelectCSVParseOptions parseOptions,
			   std::shared_ptr<Aws::S3::S3Client> s3Client,
			   bool scanOnStart,
			   bool toCache,
			   long queryId,
         std::pair<bool, std::vector<std::string>> enablePushdownProject,
         const std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> &weightedSegmentKeys);

  static std::shared_ptr<S3SelectScan> make(std::string name,
											std::string s3Bucket,
											std::string s3Object,
											std::string filterSql,
											std::string tableName,
											std::vector<std::string> columnNames,
											int64_t startOffset,
											int64_t finishOffset,
											S3SelectCSVParseOptions parseOptions,
											std::shared_ptr<Aws::S3::S3Client> s3Client,
											bool scanOnStart = true,
											bool toCache = false,
											long queryId = 0,
                      std::pair<bool, std::vector<std::string>> enablePushdownProject = std::pair<bool, std::vector<std::string>>(true, std::vector<std::string>()),
                      std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> weightedSegmentKeys = nullptr);

  size_t getProcessedBytes() const;
  size_t getReturnedBytes() const;
  size_t getNumRequests() const;

private:
  std::string s3Bucket_;
  std::string s3Object_;
  std::string filterSql_;   // "where ...."
  std::string tableName_;
  std::vector<std::string> columnNames_;    // if projection pushdown is disabled, this means the needed column names
  int64_t startOffset_;
  int64_t finishOffset_;
  S3SelectCSVParseOptions parseOptions_;
  std::shared_ptr<S3SelectParser> parser_;
  std::shared_ptr<Aws::S3::S3Client> s3Client_;
  std::vector<std::shared_ptr<std::pair<std::string, ::arrow::ArrayVector>>> columns_;  // no matter whether projection pushdown is enabled, this means the columns read from s3
  size_t processedBytes_ = 0;
  size_t returnedBytes_ = 0;
  size_t numRequests_ = 0;

  /**
   * Flags:
   * 1) scanOnStart_: send request to s3 at the beginning
   * 2) toCache_: whether the segments are to be cached
   */
  bool scanOnStart_;
  bool toCache_;
  std::pair<bool, std::vector<std::string>> enablePushdownProject_;

  void onStart();
  void onReceive(const Envelope &message) override;
  void onCacheLoadResponse(const scan::ScanMessage &message);

  [[nodiscard]] tl::expected<void, std::string> s3Select();
  [[nodiscard]] tl::expected<void, std::string> s3Scan();
  void put(const std::shared_ptr<TupleSet2> &tupleSet);

  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet);
  std::shared_ptr<TupleSet2> readTuples();
  void readAndSendTuples();
  void sendSegmentWeight();
  void generateParser();

  /**
   * used to compute filter weight
   */
  std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> weightedSegmentKeys_;
};

}

#endif //NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H
