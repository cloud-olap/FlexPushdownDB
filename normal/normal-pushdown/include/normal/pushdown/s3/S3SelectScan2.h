//
// Created by matt on 14/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_S3_S3SELECTSCAN2_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_S3_S3SELECTSCAN2_H

#include <memory>
#include <string>

#include <aws/s3/S3Client.h>

#include <normal/core/Operator.h>
#include <normal/core/message/Envelope.h>
#include <normal/tuple/FileType.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/pushdown/scan/ScanMessage.h>
#include <normal/pushdown/s3/S3SelectCSVParseOptions.h>
#include "S3SelectScanKernel.h"

using namespace Aws::S3;
using namespace normal::core;
using namespace normal::core::message;
using namespace normal::pushdown::scan;

using namespace normal::tuple;

namespace normal::pushdown {

class S3SelectScan2 : public Operator {

public:

  S3SelectScan2(std::string name,
				const std::string &s3Bucket,
				const std::string &s3Object,
				const std::string &sql,
				std::optional<int64_t> startOffset,
				std::optional<int64_t> finishOffset,
				FileType fileType,
				std::optional<std::vector<std::string>> columnNames,
				const std::optional< S3SelectCSVParseOptions> &parseOptions,
				const std::shared_ptr<S3Client> &s3Client,
				bool scanOnStart,
				long queryId);

  static std::shared_ptr<S3SelectScan2> make(const std::string &name,
											 const std::string &s3Bucket,
											 const std::string &s3Object,
											 const std::string &sql,
											 std::optional<int64_t> startOffset,
											 std::optional<int64_t> finishOffset,
											 FileType fileType,
											 const std::optional<std::vector<std::string>> &columnNames,
											 const std::optional< S3SelectCSVParseOptions> &parseOptions,
											 const std::shared_ptr<S3Client> &s3Client,
											 bool scanOnStart,
											 long queryId = 0);

  void onReceive(const Envelope &message) override;

private:
  std::optional<std::vector<std::string>> columnNames_;
  bool scanOnStart_;
  std::unique_ptr<S3SelectScanKernel> kernel_;

  [[nodiscard]] tl::expected<void, std::string> onStart();
  void onComplete(const CompleteMessage &message);
  void onScan(const ScanMessage &Message);

  void requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet);
  void readAndSendTuples(const std::vector<std::string> &columnNames);
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_S3_S3SELECTSCAN2_H
