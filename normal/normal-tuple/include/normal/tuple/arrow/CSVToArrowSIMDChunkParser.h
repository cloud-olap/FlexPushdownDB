//
// Created by Matt Woicik on 3/2/21.
//

#ifdef __AVX2__
#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_CSVTOARROWSIMDCHUNKPARSER_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_CSVTOARROWSIMDCHUNKPARSER2_H

#include <arrow/api.h>
#include <immintrin.h>
#include "normal/tuple/arrow/SIMDParserHelpers.h"
#include "normal/tuple/TupleSet.h"
#include "normal/tuple/TupleSet2.h"

class CSVToArrowSIMDChunkParser {
public:
  explicit CSVToArrowSIMDChunkParser(std::string callerName,
                                      uint64_t parseChunkSize,
                                      std::shared_ptr<arrow::Schema> schema);
  ~CSVToArrowSIMDChunkParser();

  void parseChunk(char* data, uint64_t size);
  std::shared_ptr<normal::tuple::TupleSet2> outputCompletedTupleSet();

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
  // Initialize datatypes_, arrayBuilders_, columnStartsWithQuote_, and startEndOffsets_.
  void initializeDataStructures(ParsedCSV & pcsv);

  void prettyPrintPCSV(ParsedCSV & pcsv);

  std::string callerName_;
  uint64_t parseChunkSize_;
  // Use inputstream as it provides a nice wrapper for both uncompressed and compressed data
  char* buffer_ = NULL;
  uint64_t bufferCapacity_ = 0;
  uint64_t bufferBytesUtilized_ = 0;
  ParsedCSV pcsv_;
  uint64_t rowsRead_ = 0;
  uint64_t numColumns_;
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<arrow::Type::type> datatypes_;
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> arrayBuilders_;
  std::vector<bool> columnStartsWithQuote_;
  std::vector<uint64_t> startEndOffsets_;
  std::vector<char> partial_;
  bool initialized_ = false;
};

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_CSVTOARROWSIMDPARSER2_H

#endif

