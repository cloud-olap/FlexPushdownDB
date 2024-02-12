//
// Created by matt on 12/8/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_PARQUET_LOCALPARQUETREADER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_PARQUET_LOCALPARQUETREADER_H

#include <fpdb/tuple/parquet/ParquetReader.h>
#include <fpdb/tuple/LocalFileReader.h>

namespace fpdb::tuple::parquet {

class LocalParquetReader : public LocalFileReader,
        public ParquetReader{

public:
  explicit LocalParquetReader(const std::shared_ptr<FileFormat> &format,
                              const std::shared_ptr<::arrow::Schema> &schema,
                              const std::string &path);
  ~LocalParquetReader() = default;

  static std::shared_ptr<LocalParquetReader> make(const std::shared_ptr<FileFormat> &format,
                                                  const std::shared_ptr<::arrow::Schema> &schema,
                                                  const std::string &path);

  tl::expected<std::shared_ptr<TupleSet>, std::string> read(const std::vector<std::string> &columnNames) override;

  tl::expected<std::shared_ptr<TupleSet>, std::string>
  readRange(const std::vector<std::string> &columnNames, int64_t startPos, int64_t finishPos) override;

};

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_PARQUET_LOCALPARQUETREADER_H
