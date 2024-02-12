//
// Created by Matt Woicik on 3/2/21.
//

#ifdef __AVX2__
#ifndef FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_CSVTOARROWSIMDCHUNKPARSER_H
#define FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_CSVTOARROWSIMDCHUNKPARSER_H

#include <arrow/api.h>
#include <immintrin.h>
#include "fpdb/tuple/arrow/SIMDParserHelpers.h"
#include "fpdb/tuple/TupleSet.h"
#include "fpdb/tuple/arrow/ArrowInputStream.h"

class CSVToArrowSIMDChunkParser {
public:
  explicit CSVToArrowSIMDChunkParser(uint64_t parseChunkSize,
                                     const std::shared_ptr<arrow::Schema>& inputSchema,
                                     std::shared_ptr<arrow::Schema> outputSchema,
                                     char csvFileDelimiter);
  ~CSVToArrowSIMDChunkParser();

  void parseChunk(char* data, uint64_t size);
  void parseChunk(const std::shared_ptr<arrow::io::InputStream>& inputStream);
  std::shared_ptr<fpdb::tuple::TupleSet> outputCompletedTupleSet();

  [[nodiscard]] bool isInitialized() const;

private:
  // Parse data in buffer with delimiters as filled out in pcsv. Initialize data structures if necessary and output
  // results to array builders.
  void parseAndReadInData();

  void dumpToArrayBuilderColumnWise(ParsedCSV & pcsv);

  std::string printSurroundingBufferUntilEnd(ParsedCSV & pcsv, uint64_t pcsvIndex);

  // Adds a dummy row at bufferBytesUtilized_ that contains the same number of columns as the schema passed in
  void add64ByteDummyRowToBuffer();
  // Load buffer with up to parseChunkSize_ bytes from the data passed in, update sizeRemaining and dataIndex
  // according to how many bytes read in. lastLine indicates that we are reading in the last line of the file and
  // that whatever data is in partial or data should be added to the buffer and appended with "\n" if it is not already
  // present
  void loadBuffer(char* data, uint64_t &sizeRemaining, uint64_t &dataIndex, bool lastLine);

  uint64_t initializeBufferForLoad();
  void fillBuffer(char* data, uint64_t &sizeRemaining, uint64_t &dataIndex, uint64_t copySize);
  uint64_t fillBuffer(const std::shared_ptr<arrow::io::InputStream>& inputStream, uint64_t copySize);
  void finishPreparingBufferEnd(bool lastLine);

  // Initialize datatypes_, arrayBuilders_, columnStartsWithQuote_, and startEndOffsets_.
  void initializeDataStructures(ParsedCSV & pcsv);

  [[maybe_unused]] void prettyPrintPCSV(ParsedCSV & pcsv);

  uint64_t parseChunkSize_;
  // Use inputstream as it provides a nice wrapper for both uncompressed and compressed data
  char* buffer_ = nullptr;
  uint64_t bufferCapacity_ = 0;
  uint64_t bufferBytesUtilized_ = 0;
  ParsedCSV pcsv_;
  uint64_t inputNumColumns_;
  std::shared_ptr<arrow::Schema> inputSchema_;
  std::shared_ptr<arrow::Schema> outputSchema_;
  std::vector<arrow::Type::type> datatypes_;
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> arrayBuilders_;
  std::vector<bool> columnStartsWithQuote_;
  std::vector<uint64_t> startEndOffsets_;
  std::vector<char> partial_;
  bool initialized_ = false;
  char csvFileDelimiter_;
};

#endif //FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_CSVTOARROWSIMDCHUNKPARSER_H

#endif

