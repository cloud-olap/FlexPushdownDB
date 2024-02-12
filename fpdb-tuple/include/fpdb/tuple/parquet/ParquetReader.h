//
// Created by Yifei Yang on 2/18/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_PARQUET_PARQUETREADER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_PARQUET_PARQUETREADER_H

#include <fpdb/tuple/FileReader.h>
#include <fpdb/tuple/TupleSet.h>

namespace fpdb::tuple::parquet {

class ParquetReader : virtual public FileReader {

public:
  ParquetReader() = default;
  virtual ~ParquetReader() = default;

protected:
  tl::expected<std::shared_ptr<TupleSet>, std::string>
  readImpl(const std::vector<std::string> &columnNames,
           const std::shared_ptr<::arrow::io::RandomAccessFile> &inputStream);

  tl::expected<std::shared_ptr<TupleSet>, std::string>
  readRangeImpl(const std::vector<std::string> &columnNames,
           int64_t startPos,
           int64_t finishPos,
           const std::shared_ptr<::arrow::io::RandomAccessFile> &inputStream);
};

}


#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_PARQUET_PARQUETREADER_H
