//
// Created by matt on 12/8/20.
//

#include <fpdb/tuple/FileReader.h>
#include <fpdb/util/Util.h>
#include <arrow/io/api.h>

namespace fpdb::tuple {

FileReader::FileReader(const std::shared_ptr<FileFormat> &format,
                       const std::shared_ptr<::arrow::Schema> schema):
  format_(format),
  schema_(schema) {}

const std::shared_ptr<FileFormat> &FileReader::getFormat() const {
  return format_;
}

int64_t FileReader::getBytesReadLocal() const {
  return bytesReadLocal_;
}

int64_t FileReader::getBytesReadRemote() const {
  return bytesReadRemote_;
}

tl::expected<std::shared_ptr<TupleSet>, std::string> FileReader::read() {
  return read(schema_->field_names());
}

tl::expected<std::shared_ptr<TupleSet>, std::string> FileReader::readRange(int64_t startPos, int64_t finishPos) {
  return readRange(schema_->field_names(), startPos, finishPos);
}

void FileReader::close(const std::shared_ptr<arrow::io::InputStream> &inputStream) {
  if (inputStream && !inputStream->closed()) {
    auto status = inputStream->Close();
    if (!status.ok()) {
      SPDLOG_WARN("Close input stream failed: {}", status.message());
    }
  }
}

}
