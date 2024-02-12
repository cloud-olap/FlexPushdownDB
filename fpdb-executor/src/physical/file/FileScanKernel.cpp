//
// Created by matt on 12/8/20.
//

#include <fpdb/executor/physical/file/FileScanKernel.h>

namespace fpdb::executor::physical::file {

FileScanKernel::FileScanKernel(CatalogueEntryType type,
                               const std::shared_ptr<FileFormat> &format,
                               const std::shared_ptr<::arrow::Schema> &schema,
                               int64_t fileSize,
                               const std::optional<std::pair<int64_t, int64_t>> &byteRange) :
  type_(type),
  format_(format),
  schema_(schema),
  fileSize_(fileSize),
  byteRange_(byteRange) {}

CatalogueEntryType FileScanKernel::getType() const {
  return type_;
}

const std::shared_ptr<FileFormat> &FileScanKernel::getFormat() const {
  return format_;
}

const std::shared_ptr<::arrow::Schema> &FileScanKernel::getSchema() const {
  return schema_;
}

int64_t FileScanKernel::getFileSize() const {
  return fileSize_;
}

const std::optional<std::pair<int64_t, int64_t>> &FileScanKernel::getByteRange() const {
  return byteRange_;
}

#if SHOW_DEBUG_METRICS == true
int64_t FileScanKernel::getBytesReadLocal() const {
  return bytesReadLocal_;
}

int64_t FileScanKernel::getBytesReadRemote() const {
  return bytesReadRemote_;
}

void FileScanKernel::clearBytesRead() {
  bytesReadLocal_ = 0;
  bytesReadRemote_ = 0;
}
#endif

}
