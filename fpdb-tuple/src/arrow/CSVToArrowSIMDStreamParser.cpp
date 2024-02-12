//
// Created by Matt Woicik on 2/26/21.
//

#ifdef __AVX2__

#include "fpdb/tuple/arrow/CSVToArrowSIMDStreamParser.h"
#include "fpdb/tuple/arrow/ArrowInputStream.h"
#include "fpdb/tuple/arrow/ArrowGzipInputStream2.h"
#include <arrow/util/value_parsing.h>
#include <sstream>
#include <cstdint>
#include <cstdlib>
#include <utility>

CSVToArrowSIMDStreamParser::CSVToArrowSIMDStreamParser(uint64_t parseChunkSize,
                                                       std::basic_istream<char, std::char_traits<char>> &file,
                                                       bool discardHeader,
                                                       std::shared_ptr<arrow::Schema> inputSchema,
                                                       std::shared_ptr<arrow::Schema> outputSchema,
                                                       bool gzipCompressed,
                                                       char csvFileDelimiter):
  parseChunkSize_(parseChunkSize),
  discardHeader_(discardHeader),
  inputSchema_(std::move(inputSchema)),
  outputSchema_(std::move(outputSchema)),
  csvFileDelimiter_(csvFileDelimiter) {
  if (gzipCompressed) {
    inputStream_ = std::make_shared<ArrowGzipInputStream2>(file);
  } else {
    inputStream_ = std::make_shared<ArrowInputStream>(file);
  }
  // Need to allocate allocate at least 64 bytes at end so that the last of the input can be processed since we
  // process in 64 byte chunks
  // Allocate additional 256 - 64 to pad with dummy first row and last row that are >= 64 bytes. We can likely reduce
  // this but being cautious.
  bufferCapacity_ = parseChunkSize_ + 256;
  bufferCapacity_ += 64 - bufferCapacity_ % 64;
  buffer_ = (char *) aligned_alloc(64, bufferCapacity_);
  inputNumColumns_ = inputSchema_->num_fields();
}

void CSVToArrowSIMDStreamParser::add64ByteDummyRowToBuffer() {
  int dummyColWidth = ceil(64.0 / (float) inputNumColumns_);
  for (size_t i = 0; i < inputNumColumns_; i++) {
    for (int j = 0; j < dummyColWidth - 1; j++) {
      buffer_[bufferBytesUtilized_++] = '1';
    }
    char finalChar = i == inputNumColumns_ - 1 ? 0x0a : csvFileDelimiter_;
    buffer_[bufferBytesUtilized_++] = finalChar;
  }
}


CSVToArrowSIMDStreamParser::~CSVToArrowSIMDStreamParser() {
  if (buffer_ != nullptr) {
    free(buffer_);
  }
}

uint64_t CSVToArrowSIMDStreamParser::loadBuffer() {
  bufferBytesUtilized_ = 0;
  // Need a dummy row to start the buffer to avoid special first element handling as this parser finds the indices of
  // delimiters. If we are discarding the header and we haven't done so yet we can just use that as our dummy buffer
  // row, otherwise we can create a dummy buffer row
  if (discardHeader_) {
    discardHeader_ = false;
  } else {
    add64ByteDummyRowToBuffer();
  }
  // add partial line from previous call
  uint64_t partialBytes = partial_.size();
  if (!partial_.empty()) {
    for (char i : partial_) {
      buffer_[bufferBytesUtilized_++] = i;
    }
    partial_.clear();
  }
  // don't want to overfill buffer
  uint64_t readSize = parseChunkSize_ - partialBytes;

  auto potentialBytesRead = inputStream_->Read(readSize, buffer_ + bufferBytesUtilized_);
  if (!potentialBytesRead.ok()) {
    throw std::runtime_error(
            fmt::format("Error, reading inputstream %s", potentialBytesRead.status().message().c_str()));
  }
  uint64_t fileBytesRead = potentialBytesRead.ValueOrDie();
  bufferBytesUtilized_ += fileBytesRead;
  // If the bytes in the buffer + the dummy row we will add at the end are larger than our bufferCapactity throw an
  // error
  if (bufferBytesUtilized_ + 64 >= bufferCapacity_) {
    throw std::runtime_error(
            fmt::format("Error, failed to read entire input stream, inputSize of %lu was too small", bufferCapacity_));
  }
  // save partial row in the partial vector, add at start nex time we load from buffer
  uint64_t lastBufferIndex = bufferBytesUtilized_ - 1;
  if (fileBytesRead < readSize && buffer_[lastBufferIndex] != '\n') {
    // make sure last file read ends in a new line for our parser
    buffer_[bufferBytesUtilized_++] = '\n';
  } else if (buffer_[lastBufferIndex] != '\n') {
    for (uint64_t index = lastBufferIndex; index >= 0; index--) {
      if (buffer_[index] == '\n') {
        uint64_t firstClipIndex = index + 1;
        for (uint64_t j = firstClipIndex; j <= lastBufferIndex; j++) {
          partial_.emplace_back(buffer_[j]);
        }
        bufferBytesUtilized_ = index + 1;
        break;
      }
    }
  }
  add64ByteDummyRowToBuffer();
  // now clear rest of buffer or last 64 bytes (this is necessary as leftover delimiters in memory can cause parsing issues as
  // it becomes hard to determine which rows are from this read and which ones are from previous reads.
  uint64_t bytesToZero = bufferCapacity_ - bufferBytesUtilized_ + 1 > 64 ? 64 : bufferCapacity_ - bufferBytesUtilized_ + 1;
  memset(buffer_ + bufferBytesUtilized_, 0, bytesToZero);

  return fileBytesRead + partialBytes;
}

std::string CSVToArrowSIMDStreamParser::printSurroundingBufferUntilEnd(ParsedCSV & pcsv, uint64_t pcsvIndex) {
  std::stringstream ss;
  // Print out previous 50 pcsv indices to the failed pcsvIndex and all indicues until the end of the buffer
  // If more output is needed to debug error modify this value
  for (size_t i = pcsvIndex - 50; i < pcsv.n_indexes; i++) {
    if (i != pcsv.n_indexes-1) {
      for (size_t j = pcsv.indexes[i]; j < pcsv.indexes[i+1]; j++) {
        ss << buffer_[j];
      }
    }
  }
  return ss.str();
}

void CSVToArrowSIMDStreamParser::dumpToArrayBuilderColumnWise(ParsedCSV & pcsv) {
  size_t rows = pcsv.n_indexes / inputNumColumns_ - 2; // -2 as two dummy rows at start and end

  for (int inputCol = 0; inputCol < (int) inputNumColumns_; inputCol++) {
    auto field = inputSchema_->field(inputCol);
    int outputCol = outputSchema_->GetFieldIndex(field->name());
    // No need to convert unnecessary results
    if (outputCol == -1) {
      continue;
    }
    size_t pcsvStartingIndex = inputNumColumns_ - 1 + inputCol;
    size_t startEndOffset = startEndOffsets_[outputCol];

    // Convert and append
    // TODO: Make this more generic, probably one for numerical types, boolean, and then string (utf8)
    //       Try to reduce the amount of copied code between this method and dumpToArrayBuilderRowwise
    arrow::Status status;
    switch (datatypes_[outputCol]) {
      case arrow::Type::INT32: {
        std::shared_ptr<arrow::Int32Builder> builder = std::static_pointer_cast<arrow::Int32Builder>(arrayBuilders_[outputCol]);
        for (size_t pcsvIndex = pcsvStartingIndex; pcsvIndex < rows * inputNumColumns_ - 1 + inputNumColumns_; pcsvIndex += inputNumColumns_) {
          uint64_t startingIndex = pcsv.indexes[pcsvIndex] + startEndOffset;
          uint64_t endingIndex = pcsv.indexes[pcsvIndex + 1] - startEndOffset;
          assert(endingIndex >= startingIndex);
          std::string str(buffer_ + startingIndex, endingIndex - startingIndex + 1);

          int val = std::stoi(str);
          status = builder->Append(val);
          if (!status.ok()) {
            throw std::runtime_error(status.message());
          }
        }
        break;
      }

      case arrow::Type::INT64: {
        std::shared_ptr<arrow::Int64Builder> builder = std::static_pointer_cast<arrow::Int64Builder>(arrayBuilders_[outputCol]);
        for (size_t pcsvIndex = pcsvStartingIndex; pcsvIndex < rows * inputNumColumns_ - 1 + inputNumColumns_; pcsvIndex += inputNumColumns_) {
          uint64_t startingIndex = pcsv.indexes[pcsvIndex] + startEndOffset;
          uint64_t endingIndex = pcsv.indexes[pcsvIndex + 1] - startEndOffset;
          assert(endingIndex >= startingIndex);
          std::string str(buffer_ + startingIndex, endingIndex - startingIndex + 1);

          long val = std::stol(str);
          status = builder->Append(val);
          if (!status.ok()) {
            throw std::runtime_error(status.message());
          }
        }
        break;
      }

      case arrow::Type::DOUBLE: {
        std::shared_ptr<arrow::DoubleBuilder> builder = std::static_pointer_cast<arrow::DoubleBuilder>(arrayBuilders_[outputCol]);
        for (size_t pcsvIndex = pcsvStartingIndex; pcsvIndex < rows * inputNumColumns_ - 1 + inputNumColumns_; pcsvIndex += inputNumColumns_) {
          uint64_t startingIndex = pcsv.indexes[pcsvIndex] + startEndOffset;
          uint64_t endingIndex = pcsv.indexes[pcsvIndex + 1] - startEndOffset;
          assert(endingIndex >= startingIndex);
          std::string str(buffer_ + startingIndex, endingIndex - startingIndex + 1);

          double val = std::stod(str);
          status = builder->Append(val);
          if (!status.ok()) {
            throw std::runtime_error(status.message());
          }
        }
        break;
      }

      case arrow::Type::STRING: {
        std::shared_ptr<arrow::StringBuilder> builder = std::static_pointer_cast<arrow::StringBuilder>(arrayBuilders_[outputCol]);
        for (size_t pcsvIndex = pcsvStartingIndex; pcsvIndex < rows * inputNumColumns_ - 1 + inputNumColumns_; pcsvIndex += inputNumColumns_) {
          uint64_t startingIndex = pcsv.indexes[pcsvIndex] + startEndOffset;
          uint64_t endingIndex = pcsv.indexes[pcsvIndex + 1] - startEndOffset;
          assert(endingIndex >= startingIndex);
          std::string str(buffer_ + startingIndex, endingIndex - startingIndex + 1);

          status = builder->Append(str);
          if (!status.ok()) {
            throw std::runtime_error(status.message());
          }
        }
        break;
      }

      case arrow::Type::BOOL: {
        std::shared_ptr<arrow::BooleanBuilder> builder = std::static_pointer_cast<arrow::BooleanBuilder>(arrayBuilders_[outputCol]);
        for (size_t pcsvIndex = pcsvStartingIndex; pcsvIndex < rows * inputNumColumns_ - 1 + inputNumColumns_; pcsvIndex += inputNumColumns_) {
          uint64_t startingIndex = pcsv.indexes[pcsvIndex] + startEndOffset;
          uint64_t endingIndex = pcsv.indexes[pcsvIndex + 1] - startEndOffset;
          assert(endingIndex >= startingIndex);
          std::string str(buffer_ + startingIndex, endingIndex - startingIndex + 1);

          bool val = false;
          if (str == "true" || str == "TRUE" || str == "1") {
            val = true;
          }
          status = builder->Append(val);
          if (!status.ok()) {
            throw std::runtime_error(status.message());
          }
        }
        break;
      }

      case arrow::Type::DATE64: {
        const auto &parser = arrow::TimestampParser::MakeISO8601();
        std::shared_ptr<arrow::Date64Builder> builder = std::static_pointer_cast<arrow::Date64Builder>(arrayBuilders_[outputCol]);
        for (size_t pcsvIndex = pcsvStartingIndex; pcsvIndex < rows * inputNumColumns_ - 1 + inputNumColumns_; pcsvIndex += inputNumColumns_) {
          uint64_t startingIndex = pcsv.indexes[pcsvIndex] + startEndOffset;
          uint64_t endingIndex = pcsv.indexes[pcsvIndex + 1] - startEndOffset;
          uint64_t length = endingIndex - startingIndex + 1;
          assert(endingIndex >= startingIndex);
          std::string str(buffer_ + startingIndex, length);

          int64_t val;
          (*parser)(str.c_str(), length, arrow::TimeUnit::MILLI, &val);
          status = builder->Append(val);
          if (!status.ok()) {
            throw std::runtime_error(status.message());
          }
        }
        break;
      }

      default:
        throw std::runtime_error(fmt::format("Error, arrow type not supported for SIMD processing yet for col: {}, type: {}",
                                             field->name().c_str(), field->type()->name()));
    }
  }
}

[[maybe_unused]] void CSVToArrowSIMDStreamParser::prettyPrintPCSV(ParsedCSV & pcsv) {
  std::stringstream ss;
  if (pcsv.n_indexes > 0) {
    for (size_t j = 0; j < pcsv.indexes[0]; j++) {
      ss << buffer_[j];
    }
  }
  for (size_t i = 0; i < pcsv.n_indexes; i++) {
    if ( buffer_[pcsv.indexes[i]] == '\n') {
      ss << "\n";
    } else {
      ss << csvFileDelimiter_;
    }
    if (i != pcsv.n_indexes-1) {
      for (size_t j = pcsv.indexes[i] + 1; j < pcsv.indexes[i+1]; j++) {
        ss << buffer_[j];
      }
    }
  }
}

void CSVToArrowSIMDStreamParser::initializeDataStructures(ParsedCSV & pcsv) {
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  uint64_t pcsvStartingIndex = inputNumColumns_ - 1;
  for (int outputCol = 0; outputCol < outputSchema_->num_fields(); outputCol++) {
    auto field = outputSchema_->field(outputCol);
    int inputCol = inputSchema_->GetFieldIndex(field->name());
    // No need to convert unnecessary results
    if (inputCol == -1) {
      throw std::runtime_error(fmt::format("Error, column %s missing from input schema but in output schema", field->name().c_str()));
    }
    std::shared_ptr<arrow::DataType> datatype = field->type();
    datatypes_.emplace_back(datatype->id());
    switch(datatype->id()) {
      case arrow::Type::INT32:
        arrayBuilders_.emplace_back(std::make_shared<arrow::Int32Builder>(pool));
        break;
      case arrow::Type::STRING:
        arrayBuilders_.emplace_back(std::make_shared<arrow::StringBuilder>(pool));
        break;
      case arrow::Type::INT64:
        arrayBuilders_.emplace_back(std::make_shared<arrow::Int64Builder>(pool));
        break;
      case arrow::Type::BOOL:
        arrayBuilders_.emplace_back(std::make_shared<arrow::BooleanBuilder>(pool));
        break;
      case arrow::Type::DOUBLE:
        arrayBuilders_.emplace_back(std::make_shared<arrow::DoubleBuilder>(pool));
        break;
      case arrow::Type::DATE64:
        arrayBuilders_.emplace_back(std::make_shared<arrow::Date64Builder>(pool));
        break;
      default:
        throw std::runtime_error(fmt::format("Error, arrow type not supported for SIMD processing yet for col: {}, type: {}",
                                             inputSchema_->field(inputCol)->name().c_str(), datatype->name()));
    }
    size_t firstRowColPcsvIndex = pcsvStartingIndex + inputCol;
    columnStartsWithQuote_.emplace_back(buffer_[pcsv.indexes[firstRowColPcsvIndex] + 1] == '"');
    startEndOffsets_.emplace_back(1 + columnStartsWithQuote_[outputCol]);
  }
}

std::shared_ptr<fpdb::tuple::TupleSet> CSVToArrowSIMDStreamParser::constructTupleSet() {
  uint64_t bytesReadFromFile = loadBuffer();
  if (bytesReadFromFile == 0) {
    // empty file, return empty TupleSet
    return fpdb::tuple::TupleSet::makeWithEmptyTable();
  }
  // #TODO The following allocation is taken from the source code and potentially more conservative than necessary
  //       Leaving it for now but can check if we can trim it down later
  ParsedCSV pcsv;
  pcsv.indexes = static_cast<uint32_t *>(aligned_alloc(64, bufferCapacity_));
  pcsv.n_indexes = 0;

  bool initialized = false;
  do {
    // 64 added in source code, believe it is a precaution
    find_indexes(reinterpret_cast<const uint8_t *>(buffer_), bufferBytesUtilized_ + 64, pcsv, csvFileDelimiter_);
    if (!initialized) {
      initializeDataStructures(pcsv);
      initialized = true;
    }
    dumpToArrayBuilderColumnWise(pcsv);
  } while (loadBuffer() > 0);

  free(pcsv.indexes);
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  for (const std::shared_ptr<arrow::ArrayBuilder>& arrayBuilder : arrayBuilders_) {
    arrow::Result<std::shared_ptr<arrow::Array>> result = arrayBuilder->Finish();
    if (!result.ok()) {
      throw std::runtime_error(fmt::format("Error, finishing arrow array for column %s, message: %s", result.status().message()));
    }
    arrays.emplace_back(result.ValueOrDie());
  }
  return fpdb::tuple::TupleSet::make(outputSchema_, arrays);
}

#endif