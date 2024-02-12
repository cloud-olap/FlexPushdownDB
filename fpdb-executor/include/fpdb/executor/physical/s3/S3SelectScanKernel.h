//
// Created by matt on 14/8/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_S3SELECTSCANKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_S3SELECTSCANKERNEL_H


//#include <fpdb/executor/physical/s3/S3SelectCSVParseOptions.h>
//#include <fpdb/catalogue/Table.h>
//#include <fpdb/aws/AWSClient.h>
//#include <fpdb/tuple/TupleSet.h>
//#include <fpdb/tuple/FileFormat.h>
//#include <string>
//#include <vector>
//#include <memory>
//
//#include "S3SelectAbstractScanKernel.hpp"
//
//using namespace fpdb::catalogue;
//using namespace fpdb::aws;
//using namespace fpdb::tuple;
//
//namespace fpdb::executor::physical::s3 {
//
//class S3SelectScanKernel: public S3SelectAbstractScanKernel {
//
//  typedef std::function<void(const std::shared_ptr<TupleSet> &)> TupleSetEventCallback;
//
//public:
//  S3SelectScanKernel(std::string s3Bucket,
//					 std::string s3Object,
//					 std::string sql,
//					 std::optional<int64_t> startPos,
//					 std::optional<int64_t> finishPos,
//					 std::shared_ptr<FileFormat> format,
//					 std::optional<S3SelectCSVParseOptions> csvParseOptions,
//           std::shared_ptr<Table> table,
//           std::shared_ptr<AWSClient> awsClient);
//
//  static std::unique_ptr<S3SelectScanKernel> make(const std::string &s3Bucket,
//												  const std::string &s3Object,
//												  const std::string &sql,
//												  std::optional<int64_t> startPos,
//												  std::optional<int64_t> finishPos,
//                          const std::shared_ptr<FileFormat> &format,
//												  const std::optional<S3SelectCSVParseOptions> &csvParseOptions,
//                          const std::shared_ptr<Table>& table,
//                          const std::shared_ptr<AWSClient>& awsClient);
//
//  tl::expected<std::shared_ptr<TupleSet>, std::string>
//  scan(const std::vector<std::string> &columnNames);
//
//  tl::expected<void, std::string> s3Select(const std::string &sql,
//										   const std::vector<std::string> &columnNames,
//										   const TupleSetEventCallback &tupleSetEventCallback);
//
//  [[nodiscard]] const std::string &getS3Bucket() const;
//  [[nodiscard]] const std::string &getS3Object() const;
//
//  [[nodiscard]] std::optional<int64_t> getStartPos() const;
//  [[nodiscard]] std::optional<int64_t> getFinishPos() const;
//
//private:
//  std::string s3Bucket_;
//  std::string s3Object_;
//  std::string sql_;
//  std::optional<int64_t> startPos_;
//  std::optional<int64_t> finishPos_;
//  std::shared_ptr<FileFormat> format_;
//  std::optional< S3SelectCSVParseOptions> csvParseOptions_;
//  std::shared_ptr<Table> table_;
//  std::shared_ptr<AWSClient> awsClient_;
//
//};
//
//}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_S3SELECTSCANKERNEL_H
