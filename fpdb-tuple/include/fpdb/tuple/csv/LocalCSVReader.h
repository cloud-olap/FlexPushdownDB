//
// Created by matt on 12/8/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CSV_LOCALCSVREADER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CSV_LOCALCSVREADER_H

#include <fpdb/tuple/csv/CSVReader.h>
#include <fpdb/tuple/LocalFileReader.h>

namespace fpdb::tuple::csv {

class LocalCSVReader : public LocalFileReader,
        public CSVReader {

public:
  explicit LocalCSVReader(const std::shared_ptr<FileFormat> &format,
                          const std::shared_ptr<::arrow::Schema> &schema,
                          const std::string &path);
  ~LocalCSVReader() = default;

  static std::shared_ptr<LocalCSVReader> make(const std::shared_ptr<FileFormat> &format,
                                              const std::shared_ptr<::arrow::Schema> &schema,
                                              const std::string &path);

  tl::expected<std::shared_ptr<TupleSet>, std::string>
  readRange(const std::vector<std::string> &columnNames, int64_t startPos, int64_t finishPos) override;

private:
  tl::expected<std::shared_ptr<TupleSet>, std::string>
  readUsingSimdParser(const std::vector<std::string> &columnNames) override;

  tl::expected<std::shared_ptr<TupleSet>, std::string>
  readUsingArrowApi(const std::vector<std::string> &columnNames) override;
};

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CSV_LOCALCSVREADER_H
