//
// Created by matt on 19/12/19.
//

#ifndef FPDB_FPDB_PUSHDOWN_SRC_IO_CSVPARSER_H
#define FPDB_FPDB_PUSHDOWN_SRC_IO_CSVPARSER_H

#include <map>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/csv/parser.h>
#include <tl/expected.hpp>

#include <fpdb/tuple/TupleSet.h>

namespace fpdb::tuple::csv {

class CSVParser {

public:
  CSVParser(std::shared_ptr<::arrow::io::RandomAccessFile> inputStream,
            std::shared_ptr<::arrow::Schema> schema,
            std::optional<std::vector<std::string>> columnNames,
            int64_t startOffset,
            std::optional<int64_t> finishOffset,
            int64_t bufferSize = DefaultBufferSize);

  /**
   * Parse a tuple set from the CSV file
   * @return
   */
  tl::expected<std::shared_ptr<TupleSet>, std::string> parse();

private:
  static constexpr int64_t DefaultBufferSize = 16 * 1024;

  std::shared_ptr<::arrow::io::RandomAccessFile> inputStream_;
  std::shared_ptr<::arrow::Schema> schema_;
  std::optional<std::vector<std::string>> columnNames_;
  int64_t startPos_;
  std::optional<int64_t> finishPos_;
  int64_t bufferSize_;

  /**
   * Arrow's CSV parser can't cope with a line containing only the end of a complete record.
   * Advances the input stream to the next newline or EOF, which should be the
   * beginning of the next complete record or the end of the file.
   *
   * @param inputStream
   * @return
   */
  tl::expected<std::shared_ptr<::arrow::Buffer>, std::string> advanceToNewLine();

  /**
   * Make output schema
   *
   * @return
   */
  tl::expected<std::shared_ptr<::arrow::Schema>, std::string> makeOutputSchema();

  /**
   * Extracts string arrays from a block parser
   *
   * @param blockParser
   * @return
   */
  static tl::expected<std::vector<std::shared_ptr<::arrow::Array>>, std::string>
  extractArrays(const arrow::csv::BlockParser &blockParser,
				const std::shared_ptr<::arrow::Schema>& csvFileSchema,
				const std::optional<std::vector<std::string>> &columnNamesToRead);

  /**
   * Concatenates the two given buffers
   *
   * @param buffer1
   * @param buffer2
   * @return
   */
  static tl::expected<std::shared_ptr<::arrow::Buffer>, std::string>
  concatenateBuffers(std::shared_ptr<::arrow::Buffer> buffer1, std::shared_ptr<::arrow::Buffer> buffer2);

  /**
   * Checks if the current buffer already ends with EOR, otherwise advances the input stream to EOR and appends
   * the remaining data to the buffer.
   *
   * @param buffer
   * @return
   */
  tl::expected<std::shared_ptr<::arrow::Buffer>, std::string> advanceToEOR(std::shared_ptr<::arrow::Buffer> buffer);
};

}

#endif //FPDB_FPDB_PUSHDOWN_SRC_IO_CSVPARSER_H
