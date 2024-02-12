//
// Created by Matt Woicik on 2/19/21.
//

#include "fpdb/tuple/arrow/ArrowGzipInputStream2.h"
#include "arrow/type_fwd.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/future.h"

#include <chrono>
#include <fstream>

ArrowGzipInputStream2::ArrowGzipInputStream2(std::basic_istream<char, std::char_traits<char>> &file):
  underlyingFile_(file) {
  currentZStream_.avail_in = 0;
  currentZStream_.zalloc = Z_NULL;
  currentZStream_.zfree = Z_NULL;
  currentZStream_.total_out = 0;
  if (zng_inflateInit2(&currentZStream_, (16+MAX_WBITS)) != Z_OK) {
    throw std::runtime_error("Error trying to init zstream");
  }
}

ArrowGzipInputStream2::~ArrowGzipInputStream2() {
  for (auto & allocation : allocations_) {
    free(allocation);
  }
}

void ArrowGzipInputStream2::resetZStream(int64_t bytesToRead) {
  char* compressedBytes = (char*) malloc(bytesToRead);
  allocations_.emplace_back(compressedBytes);
  underlyingFile_.read(compressedBytes, bytesToRead);
  int64_t compressedBytesRead = underlyingFile_.gcount();
  if (compressedBytesRead < bytesToRead) {
    underlyingFileEmpty_ = true;
  }
  processedCompressedBytes_ += compressedBytesRead;

  currentZStream_.next_in = (unsigned char*) compressedBytes;
  currentZStream_.avail_in = compressedBytesRead;
}

arrow::Result<int64_t> ArrowGzipInputStream2::Read(int64_t nbytes, void* out) {
  // check if this is the first call to Read, if so then initialize zstream
  if (currentZStream_.avail_in == 0) {
    resetZStream(nbytes);
  }

  int64_t bytesOutput = 0;
  currentZStream_.next_out = (unsigned char*) out;
  currentZStream_.avail_out = nbytes;

  while (bytesOutput < nbytes && (currentZStream_.avail_in > 0 || !underlyingFileEmpty_)) {
    // Inflate the outputBytes
    int64_t initialBytesOutput = currentZStream_.total_out;
    std::chrono::steady_clock::time_point startDecompressionTime = std::chrono::steady_clock::now();
    int err = zng_inflate (&currentZStream_, Z_SYNC_FLUSH);
    std::chrono::steady_clock::time_point stopDecompressionTime = std::chrono::steady_clock::now();
    decompressionTimeNS_ += std::chrono::duration_cast<std::chrono::nanoseconds>(stopDecompressionTime - startDecompressionTime).count();
    int64_t postBytesOutput = currentZStream_.total_out;
    bytesOutput += postBytesOutput - initialBytesOutput;
    currentZStream_.next_out = (unsigned char*) (out) + bytesOutput;
    currentZStream_.avail_out = nbytes - bytesOutput;
    if (err == Z_STREAM_END || currentZStream_.avail_in == 0) {
      resetZStream(nbytes);
      currentZStream_.next_out = (unsigned char*) (out) + bytesOutput;
      currentZStream_.avail_out = nbytes - bytesOutput;
    } else if (err != Z_OK) {
      throw std::runtime_error("Error reading zstream");
    }
  }
  returnedUncompressedBytes_ += bytesOutput;
  return arrow::Result<int64_t>(bytesOutput);
}



arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowGzipInputStream2::Read(int64_t nbytes) {
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
    std::chrono::steady_clock::time_point startDecompressionTime = std::chrono::steady_clock::now();
    int err = zng_inflate (&currentZStream_, Z_SYNC_FLUSH);
    std::chrono::steady_clock::time_point stopDecompressionTime = std::chrono::steady_clock::now();
    decompressionTimeNS_ += std::chrono::duration_cast<std::chrono::nanoseconds>(stopDecompressionTime - startDecompressionTime).count();
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
  returnedUncompressedBytes_ += bytesOutput;
  std::shared_ptr<arrow::Buffer> buffer = arrow::Buffer::Wrap(outputBytes, bytesOutput);
  return arrow::Result<std::shared_ptr<arrow::Buffer>>(buffer);
}

bool ArrowGzipInputStream2::closed() const {
  return false;
}

arrow::Status ArrowGzipInputStream2::Close() {
  return arrow::Status();
}

arrow::Result<int64_t> ArrowGzipInputStream2::Tell() const {
  return arrow::Result<int64_t>(processedCompressedBytes_);
}

[[maybe_unused]] int64_t ArrowGzipInputStream2::getDecompressionTimeNS() const {
  return decompressionTimeNS_;
}