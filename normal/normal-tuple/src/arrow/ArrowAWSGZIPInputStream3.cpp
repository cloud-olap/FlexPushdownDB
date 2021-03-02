//
// Created by Matt Woicik on 2/22/21.
//

#include "normal/tuple/arrow/ArrowAWSGZIPInputStream3.h"
#include "arrow/type_fwd.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/future.h"
#include <chrono>
#include <fmt/format.h>

ArrowAWSGZIPInputStream3::ArrowAWSGZIPInputStream3(std::basic_iostream<char, std::char_traits<char>> &file, int64_t inputSize){
  int64_t sizeToAllocate = inputSize * 1.1; // Allocate extra space just in case
  char* compressedBytes = (char*) malloc(sizeToAllocate);
  allocations_.emplace_back(compressedBytes);
  file.read(compressedBytes, sizeToAllocate);
  int64_t compressedBytesRead = file.gcount();
  if (compressedBytesRead == sizeToAllocate) {
    throw std::runtime_error(fmt::format("Error, failed to read entire input stream, inputSize of %lu incorrect", sizeToAllocate));
  }
  processedCompressedBytes_ += compressedBytesRead;

  // Estimate for SSB data, if using this for another dataset will need to carefully select this value or establish
  // a retry loop
  size_t outputBytesAllocation = sizeToAllocate * 3;
  outputBytes_ = (char*) malloc(outputBytesAllocation);
  allocations_.emplace_back(outputBytes_);
  libdeflate_decompressor* decompressor = libdeflate_alloc_decompressor();
  if (decompressor == NULL) {
    throw std::runtime_error("Error creating libdeflate decompressor");
  }

  size_t actual_out_nbytes_ret = 0;
  libdeflate_result res = libdeflate_gzip_decompress(decompressor, compressedBytes, compressedBytesRead,
                                                     outputBytes_, outputBytesAllocation, &actual_out_nbytes_ret);
  if (res == LIBDEFLATE_INSUFFICIENT_SPACE) {
    throw std::runtime_error("Error, didn't allocate enough bytes when performing libdeflate");
  } else if (res != LIBDEFLATE_SUCCESS) {
    throw std::runtime_error(fmt::format("Error code %s while decompressing from libdeflate", res));
  }
  outputBytesLocation_ = 0;
  outputBytesRemaining_ = actual_out_nbytes_ret;
  returnedUncompressedBytes_ += outputBytesRemaining_;
}

ArrowAWSGZIPInputStream3::~ArrowAWSGZIPInputStream3() {
  for (auto & allocation : allocations_) {
    free(allocation);
  }
}

arrow::Result<int64_t> ArrowAWSGZIPInputStream3::Read(int64_t nbytes, void* out) {
  throw std::runtime_error("Not implemented");
  return arrow::Result<int64_t>(0);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowAWSGZIPInputStream3::Read(int64_t nbytes) {
  int64_t bytesRead = outputBytesRemaining_ >= nbytes ? nbytes : outputBytesRemaining_;
  std::shared_ptr<arrow::Buffer> buffer = arrow::Buffer::Wrap(outputBytes_ + outputBytesLocation_, bytesRead);
  outputBytesLocation_ += bytesRead;
  outputBytesRemaining_ -= bytesRead;
  return arrow::Result<std::shared_ptr<arrow::Buffer>>(buffer);
}

bool ArrowAWSGZIPInputStream3::closed() const {
  return false;
}

arrow::Status ArrowAWSGZIPInputStream3::Close() {
  return arrow::Status();
}

arrow::Result<int64_t> ArrowAWSGZIPInputStream3::Tell() const {
  return arrow::Result<int64_t>(processedCompressedBytes_);
}

int64_t ArrowAWSGZIPInputStream3::getDecompressionTimeNS() {
  return decompressionTimeNS_;
}