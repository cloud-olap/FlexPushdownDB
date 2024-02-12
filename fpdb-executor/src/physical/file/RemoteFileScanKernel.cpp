//
// Created by Yifei Yang on 2/27/22.
//

#include <fpdb/executor/physical/file/RemoteFileScanKernel.h>
#include <fpdb/store/server/file/RemoteFileReaderBuilder.h>
#include <fpdb/expression/gandiva/Cast.h>

using namespace fpdb::store::server::file;
using namespace fpdb::expression::gandiva;

namespace fpdb::executor::physical::file {

RemoteFileScanKernel::RemoteFileScanKernel(const std::shared_ptr<FileFormat> &format,
                                           const std::shared_ptr<::arrow::Schema> &schema,
                                           int64_t fileSize,
                                           const std::optional<std::pair<int64_t, int64_t>> &byteRange,
                                           const std::string &bucket,
                                           const std::string &object,
                                           const std::string &host,
                                           int port):
  FileScanKernel(CatalogueEntryType::OBJ_STORE, format, schema, fileSize, byteRange),
  bucket_(bucket),
  object_(object),
  host_(host),
  port_(port) {}

std::shared_ptr<RemoteFileScanKernel>
RemoteFileScanKernel::make(const std::shared_ptr<FileFormat> &format,
                           const std::shared_ptr<::arrow::Schema> &schema,
                           int64_t fileSize,
                           const std::optional<std::pair<int64_t, int64_t>> &byteRange,
                           const std::string &bucket,
                           const std::string &object,
                           const std::string &host,
                           int port) {
  return std::make_shared<RemoteFileScanKernel>(format, schema, fileSize, byteRange, bucket, object, host, port);
}

const std::string &RemoteFileScanKernel::getBucket() const {
  return bucket_;
}

const std::string &RemoteFileScanKernel::getObject() const {
  return object_;
}

tl::expected<std::shared_ptr<TupleSet>, std::string> RemoteFileScanKernel::scan() {
  auto reader = RemoteFileReaderBuilder::make(format_, schema_, bucket_, object_, host_, port_);

  // scan
  tl::expected<std::shared_ptr<TupleSet>, std::string> expTupleSet;
  if (byteRange_.has_value()) {
    expTupleSet = reader->readRange(byteRange_->first, byteRange_->second);
  } else {
    expTupleSet = reader->read();
  }
  if (!expTupleSet.has_value()) {
    return expTupleSet;
  }

#if SHOW_DEBUG_METRICS == true
  bytesReadRemote_ += reader->getBytesReadRemote();
#endif

  // convert date32 to date64 for parquet
  if (format_->getType() == FileFormatType::PARQUET) {
    return Cast::castDate32ToDate64(*expTupleSet);
  } else {
    return expTupleSet;
  }
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
RemoteFileScanKernel::scan(const std::vector<std::string> &columnNames) {
  auto reader = RemoteFileReaderBuilder::make(format_, schema_, bucket_, object_, host_, port_);

  // scan
  tl::expected<std::shared_ptr<TupleSet>, std::string> expTupleSet;
  if (byteRange_.has_value()) {
    expTupleSet = reader->readRange(columnNames, byteRange_->first, byteRange_->second);
  } else {
    expTupleSet = reader->read(columnNames);
  }
  if (!expTupleSet.has_value()) {
    return expTupleSet;
  }

#if SHOW_DEBUG_METRICS == true
  bytesReadRemote_ += reader->getBytesReadRemote();
#endif

  // convert date32 to date64 for parquet
  if (format_->getType() == FileFormatType::PARQUET) {
    return Cast::castDate32ToDate64(*expTupleSet);
  } else {
    return expTupleSet;
  }
}

}
