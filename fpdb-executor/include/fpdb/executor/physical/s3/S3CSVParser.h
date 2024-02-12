//
// Created by matt on 14/12/19.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_S3CSVPARSER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_S3CSVPARSER_H

#include <fpdb/tuple/TupleSet.h>
#include <tl/expected.hpp>
#include <aws/core/Aws.h>
#include <string>

using namespace fpdb::tuple;

namespace fpdb::executor::physical::s3 {

class S3CSVParser {

private:

  static const int CSV_READER_BUFFER_SIZE = 128 * 1024;

  std::vector<std::string> columnNames_;
  std::shared_ptr<arrow::Schema> schema_;
  char csvDelimiter_;

  std::vector<unsigned char> partial{};

public:
  S3CSVParser(std::vector<std::string> columnNames,
				 std::shared_ptr<arrow::Schema> schema,
				 char csvDelimiter);

  static std::shared_ptr<S3CSVParser> make(const std::vector<std::string>& columnNames,
                                              const std::shared_ptr<arrow::Schema>& schema,
                                              char csvDelimiter);

  tl::expected<std::shared_ptr<TupleSet>, std::string> parseCompletePayload(
      const std::vector<unsigned char, Aws::Allocator<unsigned char>>::iterator &from,
      const std::vector<unsigned char, Aws::Allocator<unsigned char>>::iterator &to);

  tl::expected<std::optional<std::shared_ptr<TupleSet>>, std::string> parse(Aws::Vector<unsigned char> &Vector);

};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_S3_S3CSVPARSER_H
