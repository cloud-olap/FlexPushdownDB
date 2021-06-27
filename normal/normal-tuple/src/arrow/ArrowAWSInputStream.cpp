//
// Created by Matt Woicik on 2/10/21.
//

#include "normal/tuple/arrow/ArrowAWSInputStream.h"
#include "arrow/type_fwd.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/future.h"
#include <arrow/api.h>
#include <fstream>

ArrowAWSInputStream::ArrowAWSInputStream(std::basic_iostream<char, std::char_traits<char>> &file):
  underlyingFile_(file) {
}

ArrowAWSInputStream::~ArrowAWSInputStream() {
  for (auto & allocation : allocations_) {
    free(allocation);
  }
}

arrow::Result<int64_t> ArrowAWSInputStream::Read(int64_t nbytes, void* out) {
  underlyingFile_.read(static_cast<char *>(out), nbytes);
  int64_t bytesRead = underlyingFile_.gcount();
  position_ += bytesRead;
  return arrow::Result<int64_t>(bytesRead);
}


arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowAWSInputStream::Read(int64_t nbytes) {
  char* bytes = (char*) malloc(nbytes);
  allocations_.emplace_back(bytes);
  underlyingFile_.read(bytes, nbytes);
  int64_t bytesRead = underlyingFile_.gcount();
  position_ += bytesRead;
  std::shared_ptr<arrow::Buffer> buffer = arrow::Buffer::Wrap(bytes, bytesRead);
  return arrow::Result<std::shared_ptr<arrow::Buffer>>(buffer);
}

bool ArrowAWSInputStream::closed() const {
  return false;
}

arrow::Status ArrowAWSInputStream::Close() {
  return arrow::Status();
}

arrow::Result<int64_t> ArrowAWSInputStream::Tell() const {
  return arrow::Result<int64_t>(position_);
}
