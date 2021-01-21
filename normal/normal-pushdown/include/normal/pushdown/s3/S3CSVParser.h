//
// Created by matt on 14/12/19.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_S3_S3CSVPARSER_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_S3_S3CSVPARSER_H

#include <string>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/s3/S3Client.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/core/Aws.h>
#include <aws/s3/model/SelectObjectContentRequest.h>
#include <aws/core/client/ClientConfiguration.h>
#include <normal/tuple/TupleSet.h>

using namespace normal::tuple;

namespace normal::pushdown {

class S3CSVParser {

private:

  static const int CSV_READER_BUFFER_SIZE = 128 * 1024;

  std::vector<std::string> columnNames_;
  std::shared_ptr<arrow::Schema> schema_;

  std::vector<unsigned char> partial{};

public:
  S3CSVParser(std::vector<std::string> columnNames,
				 std::shared_ptr<arrow::Schema> schema);

  static std::shared_ptr<S3CSVParser> make(std::vector<std::string> columnNames,
                                              std::shared_ptr<arrow::Schema> schema);

  std::shared_ptr<TupleSet> parseCompletePayload(
      const std::vector<unsigned char, Aws::Allocator<unsigned char>>::iterator &from,
      const std::vector<unsigned char, Aws::Allocator<unsigned char>>::iterator &to);

public:

  [[deprecated("Use parse(Aws::Vector<unsigned char> &Vector)")]] std::shared_ptr<TupleSet> parsePayload(Aws::Vector<unsigned char> &Vector);
  tl::expected<std::optional<std::shared_ptr<TupleSet>>, std::string> parse(Aws::Vector<unsigned char> &Vector);

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_S3_S3SELECTPARSER_H
