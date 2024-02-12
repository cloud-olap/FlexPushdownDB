//
// Created by Yifei Yang on 2/18/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_REMOTECSVREADER_H
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_REMOTECSVREADER_H

#include <fpdb/store/server/file/RemoteFileReader.h>
#include <fpdb/tuple/csv/CSVReader.h>

using namespace fpdb::tuple;

namespace fpdb::store::server::file {

class RemoteCSVReader : public RemoteFileReader,
        public csv::CSVReader{

public:
  RemoteCSVReader(const std::shared_ptr<FileFormat> &format,
                  const std::shared_ptr<::arrow::Schema> &schema,
                  const std::string &bucket,
                  const std::string &object,
                  const std::string &host,
                  int port);
  ~RemoteCSVReader() = default;

  static std::shared_ptr<RemoteCSVReader> make(const std::shared_ptr<FileFormat> &format,
                                               const std::shared_ptr<::arrow::Schema> &schema,
                                               const std::string &bucket,
                                               const std::string &object,
                                               const std::string &host,
                                               int port);

  tl::expected<std::shared_ptr<TupleSet>, std::string>
  readRange(const std::vector<std::string> &columnNames, int64_t startPos, int64_t finishPos) override;

private:
  tl::expected<std::shared_ptr<TupleSet>, std::string>
  readUsingSimdParser(const std::vector<std::string> &columnNames) override;

  virtual tl::expected<std::shared_ptr<TupleSet>, std::string>
  readUsingArrowApi(const std::vector<std::string> &columnNames) override;
};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_REMOTECSVREADER_H
