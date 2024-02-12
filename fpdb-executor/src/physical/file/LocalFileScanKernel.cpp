//
// Created by matt on 12/8/20.
//

#include <fpdb/executor/physical/file/LocalFileScanKernel.h>
#include <fpdb/expression/gandiva/Cast.h>
#include <fpdb/tuple/LocalFileReaderBuilder.h>

using namespace fpdb::expression::gandiva;

namespace fpdb::executor::physical::file {

LocalFileScanKernel::LocalFileScanKernel(const std::shared_ptr<FileFormat> &format,
                                         const std::shared_ptr<::arrow::Schema> &schema,
                                         int64_t fileSize,
                                         const std::optional<std::pair<int64_t, int64_t>> &byteRange,
                                         const std::string &path) :
  FileScanKernel(CatalogueEntryType::LOCAL_FS, format, schema, fileSize, byteRange),
  path_(path) {}

std::shared_ptr<LocalFileScanKernel>
LocalFileScanKernel::make(const std::shared_ptr<FileFormat> &format,
                          const std::shared_ptr<::arrow::Schema> &schema,
                          int64_t fileSize,
                          const std::optional<std::pair<int64_t, int64_t>> &byteRange,
                          const std::string &path) {
  return std::make_shared<LocalFileScanKernel>(format, schema, fileSize, byteRange, path);
}

const std::string &LocalFileScanKernel::getPath() const {
  return path_;
}

void LocalFileScanKernel::setPath(const std::string &path) {
  path_ = path;
}

tl::expected<std::shared_ptr<TupleSet>, std::string> LocalFileScanKernel::scan() {
  auto reader = LocalFileReaderBuilder::make(format_, schema_, path_);

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
  bytesReadLocal_ += reader->getBytesReadLocal();
#endif

  // convert date32 to date64 for parquet
  if (format_->getType() == FileFormatType::PARQUET) {
    return Cast::castDate32ToDate64(*expTupleSet);
  } else {
    return expTupleSet;
  }
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
LocalFileScanKernel::scan(const std::vector<std::string> &columnNames) {
  auto reader = LocalFileReaderBuilder::make(format_, schema_, path_);

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
  bytesReadLocal_ += reader->getBytesReadLocal();
#endif

  // convert date32 to date64 for parquet
  if (format_->getType() == FileFormatType::PARQUET) {
    return Cast::castDate32ToDate64(*expTupleSet);
  } else {
    return expTupleSet;
  }
}

}
