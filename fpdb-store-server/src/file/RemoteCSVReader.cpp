//
// Created by Yifei Yang on 2/18/22.
//

#include <fpdb/store/server/file/RemoteCSVReader.h>
#include <fpdb/store/server/file/ArrowRemoteFileInputStream.h>
#include <streambuf>

namespace fpdb::store::server::file {

RemoteCSVReader::RemoteCSVReader(const std::shared_ptr<FileFormat> &format,
                                 const std::shared_ptr<::arrow::Schema> &schema,
                                 const std::string &bucket,
                                 const std::string &object,
                                 const std::string &host,
                                 int port) :
  FileReader(format, schema),
  RemoteFileReader(bucket, object, host, port) {}

std::shared_ptr<RemoteCSVReader> RemoteCSVReader::make(const std::shared_ptr<FileFormat> &format,
                                                       const std::shared_ptr<::arrow::Schema> &schema,
                                                       const std::string &bucket,
                                                       const std::string &object,
                                                       const std::string &host,
                                                       int port) {
  return std::make_shared<RemoteCSVReader>(format, schema, bucket, object, host, port);
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
RemoteCSVReader::readRange(const std::vector<std::string> &columnNames, int64_t startPos, int64_t finishPos) {
  // make remote input stream
  auto inputStream = ArrowRemoteFileInputStream::make(bucket_, object_, host_, port_);

  // read
  auto expTupleSet = CSVReader::readRangeImpl(columnNames, startPos, finishPos, inputStream);
  bytesReadRemote_ += inputStream->getBytesRead();

  // close
  close(inputStream);
  return expTupleSet;
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
RemoteCSVReader::readUsingSimdParser(const std::vector<std::string> &columnNames) {
  // get file size
  auto expSize = getFileSize();
  if (!expSize) {
    return tl::make_unexpected(expSize.error());
  }
  auto size = *expSize;

  // make remote input stream
  auto arrowInputStream = ArrowRemoteFileInputStream::make(bucket_, object_, host_, port_);

  // read file into bytes
  char* out = (char*) malloc(size);
  auto result = arrowInputStream->Read(size, out);
  if (!result.ok()) {
    return tl::make_unexpected(result.status().message());
  }

  // create an std::istream from bytes
  struct membuf : std::streambuf
  {
    membuf(char* begin, char* end) {
      this->setg(begin, begin, end);
    }
  };
  membuf mbuf(out, out + result.ValueOrDie());
  std::istream inputStream(&mbuf);

  // parse bytes into table
#ifdef __AVX2__
    auto expTupleSet = CSVReader::readUsingSimdParserImpl(columnNames, inputStream);
#else
    auto expTupleSet = TupleSet::makeWithEmptyTable();
#endif

  // close
  bytesReadRemote_ += arrowInputStream->getBytesRead();
  close(arrowInputStream);
  free(out);
  return expTupleSet;
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
RemoteCSVReader::readUsingArrowApi(const std::vector<std::string> &columnNames) {
  // make remote input stream
  auto inputStream = ArrowRemoteFileInputStream::make(bucket_, object_, host_, port_);

  // read
  auto expTupleSet = CSVReader::readUsingArrowApiImpl(columnNames, inputStream);
  bytesReadRemote_ += inputStream->getBytesRead();

  // close
  close(inputStream);
  return expTupleSet;
}

}
