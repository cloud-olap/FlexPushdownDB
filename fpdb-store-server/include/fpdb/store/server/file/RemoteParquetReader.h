//
// Created by Yifei Yang on 2/18/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_REMOTEPARQUETREADER_H
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_REMOTEPARQUETREADER_H

#include <fpdb/store/server/file/RemoteFileReader.h>
#include <fpdb/tuple/parquet/ParquetReader.h>

using namespace fpdb::tuple;

namespace fpdb::store::server::file {

class RemoteParquetReader : public RemoteFileReader,
        public parquet::ParquetReader {

public:
  RemoteParquetReader(const std::shared_ptr<FileFormat> &format,
                      const std::shared_ptr<::arrow::Schema> &schema,
                      const std::string &bucket,
                      const std::string &object,
                      const std::string &host,
                      int port);
  ~RemoteParquetReader() = default;

  static std::shared_ptr<RemoteParquetReader> make(const std::shared_ptr<FileFormat> &format,
                                                   const std::shared_ptr<::arrow::Schema> &schema,
                                                   const std::string &bucket,
                                                   const std::string &object,
                                                   const std::string &host,
                                                   int port);

  tl::expected<std::shared_ptr<TupleSet>, std::string> read(const std::vector<std::string> &columnNames) override;

  tl::expected<std::shared_ptr<TupleSet>, std::string>
  readRange(const std::vector<std::string> &columnNames, int64_t startPos, int64_t finishPos) override;

};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_REMOTEPARQUETREADER_H
