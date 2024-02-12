//
// Created by Yifei Yang on 2/18/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_REMOTEFILEREADERBUILDER_H
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_REMOTEFILEREADERBUILDER_H

#include <fpdb/store/server/file/RemoteFileReader.h>
#include <fpdb/tuple/FileFormat.h>

namespace fpdb::store::server::file {

class RemoteFileReaderBuilder {

public:
  static std::shared_ptr<RemoteFileReader> make(const std::shared_ptr<FileFormat> &format,
                                                const std::shared_ptr<::arrow::Schema> &schema,
                                                const std::string &bucket,
                                                const std::string &object,
                                                const std::string &host,
                                                int port);

};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FILE_REMOTEFILEREADERBUILDER_H
