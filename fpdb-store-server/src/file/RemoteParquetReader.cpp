//
// Created by Yifei Yang on 2/18/22.
//

#include <fpdb/store/server/file/RemoteParquetReader.h>
#include <fpdb/store/server/file/ArrowRemoteFileInputStream.h>

namespace fpdb::store::server::file {

RemoteParquetReader::RemoteParquetReader(const std::shared_ptr<FileFormat> &format,
                                         const std::shared_ptr<::arrow::Schema> &schema,
                                         const std::string &bucket,
                                         const std::string &object,
                                         const std::string &host,
                                         int port) :
  FileReader(format, schema),
  RemoteFileReader(bucket, object, host, port) {}

std::shared_ptr<RemoteParquetReader> RemoteParquetReader::make(const std::shared_ptr<FileFormat> &format,
                                                               const std::shared_ptr<::arrow::Schema> &schema,
                                                               const std::string &bucket,
                                                               const std::string &object,
                                                               const std::string &host,
                                                               int port) {
  return std::make_shared<RemoteParquetReader>(format, schema, bucket, object, host, port);
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
RemoteParquetReader::read(const std::vector<std::string> &columnNames) {
  // make remote input stream
  auto inputStream = ArrowRemoteFileInputStream::make(bucket_, object_, host_, port_);

  // read
  auto expTupleSet = ParquetReader::readImpl(columnNames, inputStream);
  bytesReadRemote_ += inputStream->getBytesRead();

  // close
  close(inputStream);
  return expTupleSet;
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
RemoteParquetReader::readRange(const std::vector<std::string> &columnNames, int64_t startPos, int64_t finishPos) {
  // make remote input stream
  auto inputStream = ArrowRemoteFileInputStream::make(bucket_, object_, host_, port_);

  // read
  auto expTupleSet = ParquetReader::readRangeImpl(columnNames, startPos, finishPos, inputStream);
  bytesReadRemote_ += inputStream->getBytesRead();

  // close
  close(inputStream);
  return expTupleSet;
}

}
