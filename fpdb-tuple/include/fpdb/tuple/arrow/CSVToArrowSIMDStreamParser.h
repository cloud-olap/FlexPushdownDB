//
// Created by Matt Woicik on 2/26/21.
//

#ifdef __AVX2__
#ifndef FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_CSVTOARROWSIMDSTREAMPARSER_H
#define FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_CSVTOARROWSIMDSTREAMPARSER_H

#include <arrow/api.h>
#include <immintrin.h>
#include "fpdb/tuple/arrow/SIMDParserHelpers.h"
#include "fpdb/tuple/TupleSet.h"

class CSVToArrowSIMDStreamParser {
public:
  static constexpr int DefaultParseChunkSize = 128 * 1024;

  // The header is discarded and the schema passed in is used and assumed to be valid.
  // If the header is in the file input then set discardHeader to true so that it can be ignored
  // otherwise set discardHeader to false
  // Process columns specified by outputSchema. outputSchema assumes to have the same ordering of fields as in
  // inputSchema (as this seems to be the way that we handle it already in Get, so this avoids a check in our processing)
  explicit CSVToArrowSIMDStreamParser(uint64_t parseChunkSize,
                                      std::basic_istream<char, std::char_traits<char>> &file,
                                      bool discardHeader,
                                      std::shared_ptr<arrow::Schema> inputSchema,
                                      std::shared_ptr<arrow::Schema> outputSchema,
                                      bool gzipCompressed,
                                      char csvFileDelimiter);
  ~CSVToArrowSIMDStreamParser();

  std::shared_ptr<fpdb::tuple::TupleSet> constructTupleSet();

private:

  void dumpToArrayBuilderColumnWise(ParsedCSV & pcsv);

  std::string printSurroundingBufferUntilEnd(ParsedCSV & pcsv, uint64_t pcsvIndex);

  // Adds a dummy row at bufferBytesUtilized_ that contains the same number of columns as the schema passed in
  void add64ByteDummyRowToBuffer();
  // Returns the number of bytes loaded from the file (note that this number includes the partial bytes left over from
  // an incomplete line in the previous read and any incomplete lines from the current read
  uint64_t loadBuffer();
  // Initialize datatypes_, arrayBuilders_, columnStartsWithQuote_, and startEndOffsets_.
  void initializeDataStructures(ParsedCSV & pcsv);

  [[maybe_unused]] void prettyPrintPCSV(ParsedCSV & pcsv);

  uint64_t parseChunkSize_;
  // Use inputstream as it provides a nice wrapper for both uncompressed and compressed data
  std::shared_ptr<arrow::io::InputStream> inputStream_;
  char* buffer_ = nullptr;
  uint64_t bufferCapacity_ = 0;
  uint64_t bufferBytesUtilized_ = 0;
  uint64_t inputNumColumns_;
  bool discardHeader_; // If true we ignore the first line of input
  std::shared_ptr<arrow::Schema> inputSchema_; // The schema of the CSV data passed in
  std::shared_ptr<arrow::Schema> outputSchema_; // The output schema to produce, ignoring converting any columns omitted from inputSchema_
  std::vector<arrow::Type::type> datatypes_;
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> arrayBuilders_;
  std::vector<bool> columnStartsWithQuote_;
  std::vector<uint64_t> startEndOffsets_;
  std::vector<char> partial_;
  char csvFileDelimiter_;
};

#endif //FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_CSVTOARROWSIMDSTREAMPARSER_H

#endif
