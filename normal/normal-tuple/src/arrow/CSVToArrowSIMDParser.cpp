//
// Created by Matt Woicik on 2/26/21.
//

#ifdef __AVX2__

#include "normal/tuple/arrow/CSVToArrowSIMDParser.h"
#include "normal/tuple/arrow/ArrowAWSInputStream.h"
#include "normal/tuple/arrow/ArrowAWSGZIPInputStream2.h"
#include <spdlog/common.h>
#include <sstream>
#include <cstdint>
#include <cstdlib>

CSVToArrowSIMDParser::CSVToArrowSIMDParser(std::string &callerName,
                                           uint64_t parseChunkSize,
                                           std::basic_iostream<char, std::char_traits<char>> &file,
                                           bool discardHeader,
                                           std::shared_ptr<arrow::Schema> schema,
                                           bool gzipCompressed):
  callerName_(std::move(callerName)),
  parseChunkSize_(parseChunkSize),
  discardHeader_(discardHeader),
  schema_(schema) {
  if (gzipCompressed) {
    inputStream_ = std::make_shared<ArrowAWSGZIPInputStream2>(file);
  } else {
    inputStream_ = std::make_shared<ArrowAWSInputStream>(file);
  }
  // Need to allocate allocate at least 64 bytes at end so that the last of the input can be processed since we
  // process in 64 byte chunks
  // Allocate additional 256 - 64 to pad with dummy first row and last row that are >= 64 bytes. We can likely reduce
  // this but being cautious.
  bufferCapacity_ = parseChunkSize_ + 256;
  bufferCapacity_ += 64 - bufferCapacity_ % 64;
  buffer_ = (char *) aligned_alloc(64, bufferCapacity_);
}

void CSVToArrowSIMDParser::add64ByteDummyRowToBuffer() {
  int numCols = schema_->num_fields();
  int dummyColWidth = ceil(64.0 / (float) numCols);
  assert(numCols > 0);
  for (int i = 0; i < numCols; i++) {
    for (int j = 0; j < dummyColWidth - 1; j++) {
      buffer_[bufferBytesUtilized_++] = '1';
    }
    char finalChar = i == numCols - 1 ? 0x0a : ',';
    buffer_[bufferBytesUtilized_++] = finalChar;
    bufferBytesUtilized_++;
  }
}


CSVToArrowSIMDParser::~CSVToArrowSIMDParser() {
  if (buffer_ != NULL) {
    free(buffer_);
  }
}

uint64_t CSVToArrowSIMDParser::loadBuffer() {
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
    for (int i = 0; i < partial_.size(); i++) {
      buffer_[bufferBytesUtilized_++] = partial_.at(i);
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
  // now clear rest of buffer (this is necessary as leftover "," in memory can cause parsing issues as it becomes
  // hard to determine which rows are from this read and which ones are from previous reads.
  memset(buffer_ + bufferBytesUtilized_, 0, bufferCapacity_ - bufferBytesUtilized_ + 1);

  return fileBytesRead + partialBytes;
}

std::string CSVToArrowSIMDParser::printSurroundingBufferUntilEnd(ParsedCSV & pcsv, uint64_t pcsvIndex) {
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

void CSVToArrowSIMDParser::dumpToArrayBuilderColumnWise(ParsedCSV & pcsv) {
  int columns = schema_->num_fields();
  int rows = pcsv.n_indexes / columns - 2; // -2 as two dummy rows at start and end

  for (int col = 0; col < schema_->num_fields(); col++) {
    auto field = schema_->field(col);
    int64_t pcsvStartingIndex = columns - 1 + col;
    int64_t startEndOffset = startEndOffsets_[col];
    // TODO: Make this more generic, probably one for numerical types, boolean, and then string (utf8)
    //       Try to reduce the amount of copied code between this method and dumpToArrayBuilderRowwise
    if (datatypes_[col] == arrow::Type::INT32) {
      std::shared_ptr<arrow::Int32Builder> builder = std::static_pointer_cast<arrow::Int32Builder>(arrayBuilders_[col]);
      for (int pcsvIndex = pcsvStartingIndex; pcsvIndex < rows * columns - 1 + columns; pcsvIndex += columns) {
        uint64_t startingIndex = pcsv.indexes[pcsvIndex] + startEndOffset;
        uint64_t endingIndex = pcsv.indexes[pcsvIndex + 1] - startEndOffset;
        assert(endingIndex >= startingIndex);
        bool negative = buffer_[startingIndex] == '-';
        startingIndex += negative;
        int val = 0;
        while (startingIndex <= endingIndex) {
          char currentChar = buffer_[startingIndex++];
          val = val * 10 + currentChar - '0';
        }
        // Bit trick to conditionally negate, though likely compiler already does this
        // source: https://graphics.stanford.edu/~seander/bithacks.html#ConditionalNegate
        val = (val ^ -negative) + negative;
        builder->Append(val);
      }
    } else if (datatypes_[col] == arrow::Type::INT64) {
      std::shared_ptr<arrow::Int64Builder> builder = std::static_pointer_cast<arrow::Int64Builder>(arrayBuilders_[col]);
      for (int pcsvIndex = pcsvStartingIndex; pcsvIndex < rows * columns - 1 + columns; pcsvIndex += columns) {
        uint64_t startingIndex = pcsv.indexes[pcsvIndex] + startEndOffset;
        uint64_t endingIndex = pcsv.indexes[pcsvIndex + 1] - startEndOffset;
        assert(endingIndex >= startingIndex);
        bool negative = buffer_[startingIndex] == '-';
        startingIndex += negative;
        int64_t val = 0;
        while (startingIndex <= endingIndex) {
          char currentChar = buffer_[startingIndex++];
          val = val * 10 + currentChar - '0';
        }
        // Bit trick to conditionally negate
        // source: https://graphics.stanford.edu/~seander/bithacks.html#ConditionalNegate
        val = (val ^ -negative) + negative;
        builder->Append(val);
      }
    } else if (datatypes_[col] == arrow::Type::STRING) {
      std::shared_ptr<arrow::StringBuilder> builder = std::static_pointer_cast<arrow::StringBuilder>(arrayBuilders_[col]);
      for (int pcsvIndex = pcsvStartingIndex; pcsvIndex < rows * columns - 1 + columns; pcsvIndex += columns) {
        uint64_t startingIndex = pcsv.indexes[pcsvIndex] + startEndOffset;
        uint64_t endingIndex = pcsv.indexes[pcsvIndex + 1] - startEndOffset;
        try {
          assert(endingIndex >= startingIndex);
          std::string val(buffer_ + startingIndex, endingIndex - startingIndex + 1);
          builder->Append(val);
        } catch (std::exception& e) {
          std::string fullValue(buffer_ + pcsv.indexes[pcsvIndex], pcsv.indexes[pcsvIndex + 1] - pcsv.indexes[pcsvIndex] + 1);
          std::string remainingBuffer = printSurroundingBufferUntilEnd(pcsv, pcsvIndex);
          SPDLOG_ERROR("Got exception reading utf8\n"
                       "pcsvStartingIndex: {}, pcsvEndingIndex: {}, pcsvIndices: {}\n"
                       "startingIndex: {}, size was {}, full value is: {}\n"
                       "nonDummyColumns: {}, nonDummyRows: {}\n"
                       "error: {}\n"
                       "remaining buffer:\n{}\nEnd of remaining buffer", pcsvIndex, pcsvIndex + 1, pcsv.n_indexes,
                       startingIndex, endingIndex - startingIndex + 1, fullValue,
                       columns, rows, e.what(), remainingBuffer);
        }
      }
    } else if (datatypes_[col] == arrow::Type::BOOL) {
      std::shared_ptr<arrow::BooleanBuilder> builder = std::static_pointer_cast<arrow::BooleanBuilder>(arrayBuilders_[col]);
      for (int pcsvIndex = pcsvStartingIndex; pcsvIndex < rows * columns - 1 + columns; pcsvIndex += columns) {
        uint64_t startingIndex = pcsv.indexes[pcsvIndex] + startEndOffset;
        uint64_t endingIndex = pcsv.indexes[pcsvIndex + 1] - startEndOffset;
        assert(endingIndex >= startingIndex);
        std::string val(buffer_ + startingIndex, endingIndex - startingIndex + 1);
        bool boolVal = false;
        if (strcmp(val.c_str(), "true") || val == "1") {
          boolVal = true;
        }
        builder->Append(boolVal);
      }
    } else {
      throw std::runtime_error(fmt::format("Error, arrow type not supported for SIMD processing yet: %s", schema_->field(col)->type()->name().c_str()));
    }
  }
}

void CSVToArrowSIMDParser::dumpToArrayBuilderRowWise(ParsedCSV & pcsv) {
  uint32_t columns = schema_->num_fields();
  uint32_t rows = (pcsv.n_indexes / columns) - 2; // -2 as two dummy rows at start and end
  uint64_t pcsvStartingIndex = columns - 1;

  int currentColumn = 0;
  for (int pcsvIndex = pcsvStartingIndex; pcsvIndex < rows * columns - 1 + columns; pcsvIndex++) {
    uint64_t col = currentColumn++ % columns;
    uint64_t startingIndex = pcsv.indexes[pcsvIndex] + startEndOffsets_[col];
    uint64_t endingIndex = pcsv.indexes[pcsvIndex + 1] - startEndOffsets_[col];
    assert(endingIndex >= startingIndex);
    if (datatypes_[col] == arrow::Type::INT32) {
      std::shared_ptr<arrow::Int32Builder> builder = std::static_pointer_cast<arrow::Int32Builder>(arrayBuilders_[col]);
      bool negative = buffer_[startingIndex] == '-';
      startingIndex += negative;
      int32_t val = 0;
      while (startingIndex <= endingIndex) {
        char currentChar = buffer_[startingIndex++];
        val = val * 10 + currentChar - '0';
      }
      // Bit trick to conditionally negate, though likely compiler already does thisi
      // source: https://graphics.stanford.edu/~seander/bithacks.html#ConditionalNegate
      val = (val ^ -negative) + negative;
      builder->Append(val);
    } else if (datatypes_[col] == arrow::Type::STRING) {
      std::shared_ptr<arrow::StringBuilder> builder = std::static_pointer_cast<arrow::StringBuilder>(arrayBuilders_[col]);
      try {
        assert(endingIndex >= startingIndex);
        std::string val(buffer_ + startingIndex, endingIndex - startingIndex + 1);
        builder->Append(val);
      } catch (std::exception& e) {
          std::string fullValue(buffer_ + pcsv.indexes[pcsvIndex], pcsv.indexes[pcsvIndex + 1] - pcsv.indexes[pcsvIndex] + 1);
          SPDLOG_ERROR("Got exception reading utf8\n"
                       "Caller was: {}\n"
                       "Rows: {}, cols: {}, first valid pcsvindex: {}, first invalid pcsv index: {}\n"
                       "pcsvStartingIndex: {}, pcsvEndingIndex: {}, pcsvIndices: {}\n"
                       "startingIndex in buffer: {}, size was {}, full value is: {}\n"
                       "error: {}", callerName_, rows, columns, columns - 1, rows * columns - 1 + columns, pcsvIndex, pcsvIndex + 1, pcsv.n_indexes,
                       startingIndex, endingIndex - startingIndex + 1, fullValue, e.what());
      }
    } else if (datatypes_[col] == arrow::Type::INT64) {
      std::shared_ptr<arrow::Int64Builder> builder = std::static_pointer_cast<arrow::Int64Builder>(arrayBuilders_[col]);
      bool negative = buffer_[startingIndex] == '-';
      startingIndex += negative;
      int64_t val = 0;
      while (startingIndex <= endingIndex) {
        char currentChar = buffer_[startingIndex++];
        val = val * 10 + currentChar - '0';
      }
      // Bit trick to conditionally negate, though likely compiler already does this
      // source: https://graphics.stanford.edu/~seander/bithacks.html#ConditionalNegate
      val = (val ^ -negative) + negative;
      builder->Append(val);
    } else if (datatypes_[col] == arrow::Type::BOOL) {
      std::shared_ptr<arrow::BooleanBuilder> builder = std::static_pointer_cast<arrow::BooleanBuilder>(arrayBuilders_[col]);
      std::string val(buffer_ + startingIndex, endingIndex - startingIndex + 1);
      bool boolVal = false;
      if (strcmp(val.c_str(), "true") || val == "1") {
        boolVal = true;
      }
      builder->Append(boolVal);
    } else {
      throw std::runtime_error(fmt::format("Error, arrow type not supported for SIMD processing yet for column: ", schema_->field(col)->name().c_str()));
    }
  }
}

void CSVToArrowSIMDParser::prettyPrintPCSV(ParsedCSV & pcsv) {
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
      ss << ",";
    }
    if (i != pcsv.n_indexes-1) {
      for (size_t j = pcsv.indexes[i] + 1; j < pcsv.indexes[i+1]; j++) {
        ss << buffer_[j];
      }
    }
  }
  SPDLOG_DEBUG("Buffer contains:\n{}", ss.str());
}

void CSVToArrowSIMDParser::initializeDataStructures(ParsedCSV & pcsv) {
  uint32_t columns = schema_->num_fields();
  uint32_t rows = (pcsv.n_indexes / columns) - 2; // -2 as two dummy rows at start and end
  // if rows is == 0 and we called this something went wrong in an earlier step.
  assert(rows > 0);

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  uint64_t pcsvStartingIndex = columns - 1;
  for (int col = 0; col < columns; col++) {
    auto field = schema_->field(col);
    SPDLOG_DEBUG("Initializing column: {}", field->name());
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
      default:
        throw std::runtime_error(fmt::format("Error, arrow type not supported for SIMD processing yet for col: %s", schema_->field(col)->name().c_str()));
    }
    int64_t firstRowColPcsvIndex = pcsvStartingIndex + col;
    columnStartsWithQuote_.emplace_back(buffer_[pcsv.indexes[firstRowColPcsvIndex] + 1] == '"');
    startEndOffsets_.emplace_back(1 + columnStartsWithQuote_[col]);
  }
}

std::shared_ptr<normal::tuple::TupleSet2> CSVToArrowSIMDParser::constructTupleSet() {
  uint64_t bytesReadFromFile = loadBuffer();
  if (bytesReadFromFile == 0) {
    // empty file, return empty TupleSet
    return normal::tuple::TupleSet2::make2();
  }
  // #TODO The following allocation is taken from the source code and potentially more conservative than necessary
  //       Leaving it for now but can check if we can trim it down later
  ParsedCSV pcsv;
  pcsv.indexes = static_cast<uint32_t *>(aligned_alloc(64, bufferCapacity_));
  pcsv.n_indexes = 0;

  uint32_t columns = schema_->num_fields();
  uint64_t rows = 0;

  bool initialized = false;
  do {
    // 64 added in source code, believe it is a precaution
    find_indexes(reinterpret_cast<const uint8_t *>(buffer_), bufferBytesUtilized_ + 64, pcsv);
    rows += (pcsv.n_indexes / columns) - 2; // -2 as two dummy rows at start and end
    if (!initialized) {
      initializeDataStructures(pcsv);
      initialized = true;
    }

    try {
      dumpToArrayBuilderColumnWise(pcsv);
//      dumpToArrayBuilderRowWise(pcsv);
    } catch (std::exception &e) {
      SPDLOG_ERROR("Got exception reading SIMD input: {}", e.what());
    }
  } while (loadBuffer() > 0);

  free(pcsv.indexes);
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  for (std::shared_ptr<arrow::ArrayBuilder> arrayBuilder : arrayBuilders_) {
    arrow::Result<std::shared_ptr<arrow::Array>> result = arrayBuilder->Finish();
    if (!result.ok()) {
      throw std::runtime_error(fmt::format("Error, finishing arrow array for column %s, message: %s", result.status().message()));
    }
    arrays.emplace_back(result.ValueOrDie());
  }
  std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema_, arrays, rows);
  auto tupleSetV1 = normal::tuple::TupleSet::make(table);
  return normal::tuple::TupleSet2::create(tupleSetV1);
}

#endif