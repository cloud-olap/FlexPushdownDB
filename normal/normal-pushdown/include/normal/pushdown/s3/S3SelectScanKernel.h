//
// Created by matt on 14/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_S3_S3SELECTSCANKERNEL_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_S3_S3SELECTSCANKERNEL_H

#include <string>
#include <vector>
#include <memory>

#include <aws/s3/S3Client.h>

#include <normal/tuple/TupleSet2.h>
#include <normal/tuple/FileType.h>
#include "S3SelectCSVParseOptions.h"

using namespace Aws::S3;
using namespace normal::tuple;

namespace normal::pushdown::s3 {

class S3SelectScanKernel {

  typedef std::function<void(const std::shared_ptr<TupleSet2> &)> TupleSetEventCallback;

public:
  S3SelectScanKernel(std::string s3Bucket,
					 std::string s3Object,
					 std::string sql,
					 std::optional<int64_t> startPos,
					 std::optional<int64_t> finishPos,
					 FileType fileType,
					 std::optional<S3SelectCSVParseOptions> csvParseOptions,
					 std::shared_ptr<S3Client> s3Client,
					 std::shared_ptr<arrow::Schema> schema);

  static std::unique_ptr<S3SelectScanKernel> make(const std::string &s3Bucket,
												  const std::string &s3Object,
												  const std::string &sql,
												  std::optional<int64_t> startPos,
												  std::optional<int64_t> finishPos,
												  FileType fileType,
												  const std::optional<S3SelectCSVParseOptions> &csvParseOptions,
												  const std::shared_ptr<S3Client> &s3Client,
												  const std::shared_ptr<arrow::Schema>& schema);

  tl::expected<std::shared_ptr<TupleSet2>, std::string>
  scan(const std::vector<std::string> &columnNames);

  tl::expected<void, std::string> s3Select(const std::string &sql,
										   const std::vector<std::string> &columnNames,
										   const TupleSetEventCallback &tupleSetEventCallback);

  [[nodiscard]] const std::string &getS3Bucket() const;
  [[nodiscard]] const std::string &getS3Object() const;

  [[nodiscard]] std::optional<int64_t> getStartPos() const;
  [[nodiscard]] std::optional<int64_t> getFinishPos() const;

private:
  std::string s3Bucket_;
  std::string s3Object_;
  std::string sql_;
  std::optional<int64_t> startPos_;
  std::optional<int64_t> finishPos_;
  FileType fileType_;
  std::optional< S3SelectCSVParseOptions> csvParseOptions_;
  std::shared_ptr<S3Client> s3Client_;
  std::shared_ptr<arrow::Schema> schema_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_S3_S3SELECTSCANKERNEL_H
