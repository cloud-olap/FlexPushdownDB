//
// Created by Yifei Yang on 2/18/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_REMOTEFILEREADER_H
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_REMOTEFILEREADER_H

#include <fpdb/tuple/FileReader.h>

using namespace fpdb::tuple;

namespace fpdb::store::server::file {

class RemoteFileReader : virtual public FileReader {

public:
  RemoteFileReader(const std::string &bucket,
                   const std::string &object,
                   const std::string &host,
                   int port);
  virtual ~RemoteFileReader() = default;

  tl::expected<int64_t, std::string> getFileSize() const override;

protected:
  std::string bucket_;
  std::string object_;
  std::string host_;
  int port_;
};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_REMOTEFILEREADER_H
