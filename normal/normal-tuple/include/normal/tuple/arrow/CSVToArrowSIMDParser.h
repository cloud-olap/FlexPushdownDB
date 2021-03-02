//
// Created by Matt Woicik on 2/26/21.
//

#ifdef __AVX2__
#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_CSVTOARROWSIMDPARSER2_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_CSVTOARROWSIMDPARSER2_H

#include <arrow/api.h>
#include <immintrin.h>
#include "normal/tuple/arrow/SIMDParserHelpers.h"
#include "normal/tuple/TupleSet.h"
#include "normal/tuple/TupleSet2.h"

class CSVToArrowSIMDParser {
public:
  // The header is discarded and the schema passed in is used and assumed to be valid.
  // If the header is in the file input then set discardHeader to true so that it can be ignored
  // otherwise set discardHeader to false
  explicit CSVToArrowSIMDParser(std::string &callerName,
                                uint64_t parseChunkSize,
                                std::basic_iostream<char, std::char_traits<char>> &file,
                                bool discardHeader,
                                std::shared_ptr<arrow::Schema> schema,
                                bool gzipCompressed);
  ~CSVToArrowSIMDParser();

  std::shared_ptr<normal::tuple::TupleSet2> constructTupleSet();

private:

  void dumpToArrayBuilderColumnWise(ParsedCSV & pcsv);
  void dumpToArrayBuilderRowWise(ParsedCSV & pcsv);

  std::string printSurroundingBufferUntilEnd(ParsedCSV & pcsv, uint64_t pcsvIndex);

  // Adds a dummy row at bufferBytesUtilized_ that contains the same number of columns as the schema passed in
  void add64ByteDummyRowToBuffer();
  // Returns the number of bytes loaded from the file (note that this number includes the partial bytes left over from
  // an incomplete line in the previous read and any incomplete lines from the current read
  uint64_t loadBuffer();
  // Initialize datatypes_, arrayBuilders_, columnStartsWithQuote_, and startEndOffsets_.
  void initializeDataStructures(ParsedCSV & pcsv);

  void prettyPrintPCSV(ParsedCSV & pcsv);

  std::string callerName_;
  uint64_t parseChunkSize_;
  // Use inputstream as it provides a nice wrapper for both uncompressed and compressed data
  std::shared_ptr<arrow::io::InputStream> inputStream_;
  char* buffer_ = NULL;
  uint64_t bufferCapacity_ = 0;
  uint64_t bufferBytesUtilized_ = 0;
  bool discardHeader_; // If true we ignore the first line of input
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<arrow::Type::type> datatypes_;
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> arrayBuilders_;
  std::vector<bool> columnStartsWithQuote_;
  std::vector<uint64_t> startEndOffsets_;
  std::vector<char> partial_;
};

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_CSVTOARROWSIMDPARSER2_H

#endif
