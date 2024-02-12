//
// Created by Matt Woicik on 2/10/21.
//

#include "fpdb/tuple/arrow/ArrowInputStream.h"
#include "arrow/type_fwd.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/future.h"
#include <arrow/api.h>
#include <fstream>

ArrowInputStream::ArrowInputStream(std::basic_istream<char, std::char_traits<char>> &file):
  underlyingFile_(file) {
}

ArrowInputStream::~ArrowInputStream() {
  for (auto & allocation : allocations_) {
    free(allocation);
  }
}

arrow::Result<int64_t> ArrowInputStream::Read(int64_t nbytes, void* out) {
  underlyingFile_.read(static_cast<char *>(out), nbytes);
  int64_t bytesRead = underlyingFile_.gcount();
  position_ += bytesRead;
  return arrow::Result<int64_t>(bytesRead);
}


arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowInputStream::Read(int64_t nbytes) {
  char* bytes = (char*) malloc(nbytes);
  allocations_.emplace_back(bytes);
  underlyingFile_.read(bytes, nbytes);
  int64_t bytesRead = underlyingFile_.gcount();
  position_ += bytesRead;
  std::shared_ptr<arrow::Buffer> buffer = arrow::Buffer::Wrap(bytes, bytesRead);
  return arrow::Result<std::shared_ptr<arrow::Buffer>>(buffer);
}

bool ArrowInputStream::closed() const {
  return false;
}

arrow::Status ArrowInputStream::Close() {
  return arrow::Status();
}

arrow::Result<int64_t> ArrowInputStream::Tell() const {
  return arrow::Result<int64_t>(position_);
}
