//
// Created by Matt Woicik on 2/15/21.
//

//
// Created by Matt Woicik on 2/10/21.
//

#include "normal/tuple/arrow/ArrowAWSGZIPInputStream.h"
#include "arrow/type_fwd.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/future.h"

ArrowAWSGZIPInputStream::ArrowAWSGZIPInputStream(std::basic_iostream<char, std::char_traits<char>> &file):
  underlyingFile_(file) {
  currentZStream_.avail_in = 0;
  currentZStream_.zalloc = Z_NULL;
  currentZStream_.zfree = Z_NULL;
  currentZStream_.total_out = 0;
  if (inflateInit2(&currentZStream_, (16+MAX_WBITS)) != Z_OK) {
    throw std::runtime_error("Error trying to init zstream");
  }
}

ArrowAWSGZIPInputStream::~ArrowAWSGZIPInputStream() {
  for (auto & allocation : allocations_) {
    free(allocation);
  }
}

void ArrowAWSGZIPInputStream::resetZStream(int64_t bytesToRead) {
  char* compressedBytes = (char*) malloc(bytesToRead);
  allocations_.emplace_back(compressedBytes);
  underlyingFile_.read(compressedBytes, bytesToRead);
  int64_t compressedBytesRead = underlyingFile_.gcount();
  if (compressedBytesRead < bytesToRead) {
    underlyingFileEmpty_ = true;
  }
  position_ += compressedBytesRead;

  currentZStream_.next_in = (unsigned char*) compressedBytes;
  currentZStream_.avail_in = compressedBytesRead;
}

arrow::Result<int64_t> ArrowAWSGZIPInputStream::Read(int64_t nbytes, void* out) {
  throw std::runtime_error("Not implemented");
  return arrow::Result<int64_t>(0);
}



arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowAWSGZIPInputStream::Read(int64_t nbytes) {
  // check if this is the first call to Read, if so then initialize zstream
  if (currentZStream_.avail_in == 0) {
    resetZStream(nbytes);
  }

  int64_t bytesOutput = 0;
  char* outputBytes = (char*) malloc(nbytes);
  allocations_.emplace_back(outputBytes);
  currentZStream_.next_out = (unsigned char*) outputBytes;
  currentZStream_.avail_out = nbytes;

  while (bytesOutput < nbytes && (currentZStream_.avail_in > 0 || !underlyingFileEmpty_)) {
    // Inflate the outputBytes
    int64_t initialBytesOutput = currentZStream_.total_out;
    int err = inflate (&currentZStream_, Z_SYNC_FLUSH);
    int64_t postBytesOutput = currentZStream_.total_out;
    bytesOutput += postBytesOutput - initialBytesOutput;
    currentZStream_.next_out = (unsigned char*) (outputBytes + bytesOutput);
    currentZStream_.avail_out = nbytes - bytesOutput;
    if (err == Z_STREAM_END || currentZStream_.avail_in == 0) {
      resetZStream(nbytes);
      currentZStream_.next_out = (unsigned char*) (outputBytes + bytesOutput);
      currentZStream_.avail_out = nbytes - bytesOutput;
    } else if (err != Z_OK) {
      throw std::runtime_error("Error reading zstream");
    }
  }

  std::shared_ptr<arrow::Buffer> buffer = arrow::Buffer::Wrap(outputBytes, bytesOutput);
  return arrow::Result<std::shared_ptr<arrow::Buffer>>(buffer);
}

bool ArrowAWSGZIPInputStream::closed() const {
  return false;
}

arrow::Status ArrowAWSGZIPInputStream::Close() {
  return arrow::Status();
}

arrow::Result<int64_t> ArrowAWSGZIPInputStream::Tell() const {
  return arrow::Result<int64_t>(position_);
}
