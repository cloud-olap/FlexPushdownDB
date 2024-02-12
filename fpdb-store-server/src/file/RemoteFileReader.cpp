//
// Created by Yifei Yang on 2/18/22.
//

#include <fpdb/store/server/file/RemoteFileReader.h>
#include <fpdb/store/server/file/ArrowRemoteFileInputStream.h>

namespace fpdb::store::server::file {

RemoteFileReader::RemoteFileReader(const std::string &bucket,
                                   const std::string &object,
                                   const std::string &host,
                                   int port):
  bucket_(bucket),
  object_(object),
  host_(host),
  port_(port) {}

tl::expected<int64_t, std::string> RemoteFileReader::getFileSize() const {
  // make remote input stream
  auto inputStream = ArrowRemoteFileInputStream::make(bucket_, object_, host_, port_);

  // get size
  auto expSize = inputStream->GetSize();
  if (expSize.ok()) {
    return expSize.ValueOrDie();
  } else {
    return tl::make_unexpected(expSize.status().message());
  }
}

}
