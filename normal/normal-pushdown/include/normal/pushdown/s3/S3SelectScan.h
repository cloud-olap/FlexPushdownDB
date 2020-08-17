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

#include "normal/core/Operator.h"
#include "normal/tuple/TupleSet.h"
#include "normal/pushdown/s3/S3SelectCSVParseOptions.h"

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
			   std::vector<std::string> columnNames,
			   int64_t startOffset,
			   int64_t finishOffset,
			   S3SelectCSVParseOptions parseOptions,
			   std::shared_ptr<Aws::S3::S3Client> s3Client,
			   bool scanOnStart,
			   bool toCache);

  static std::shared_ptr<S3SelectScan> make(std::string name,
											std::string s3Bucket,
											std::string s3Object,
											std::string filterSql,
											std::vector<std::string> columnNames,
											int64_t startOffset,
											int64_t finishOffset,
											S3SelectCSVParseOptions parseOptions,
											std::shared_ptr<Aws::S3::S3Client> s3Client,
											bool scanOnStart = true,
											bool toCache = false);


private:
  std::string s3Bucket_;
  std::string s3Object_;
  std::string filterSql_;   // "where ...."
  std::vector<std::string> columnNames_;
  int64_t startOffset_;
  int64_t finishOffset_;
  S3SelectCSVParseOptions parseOptions_;
  std::shared_ptr<Aws::S3::S3Client> s3Client_;
  std::vector<std::shared_ptr<std::pair<std::string, ::arrow::ArrayVector>>> columns_;

  /**
   * Flags:
   * 1) scanOnStart_: send request to s3 at the beginning
   * 2) toCache_: whether the segments are to be cached
   * 3) resultNeeded_: whether the segments will be used in the current query
   */
  bool scanOnStart_;
  bool toCache_;
  bool resultNeeded_;

  void onStart();
  void onReceive(const Envelope &message) override;
  void onCacheLoadResponse(const scan::ScanMessage &message);

  [[nodiscard]] tl::expected<void, std::string> s3Select(const TupleSetEventCallback &tupleSetEventCallback);

  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet);
  std::shared_ptr<TupleSet2> readTuples();
  void readAndSendTuples();

};

}

#endif //NORMAL_NORMAL_CORE_SRC_S3SELECTSCAN_H
