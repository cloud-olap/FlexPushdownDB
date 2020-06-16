//
// Created by matt on 19/12/19.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_IO_CSVPARSER_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_IO_CSVPARSER_H

#include <map>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/csv/parser.h>
#include <tl/expected.hpp>

#include <normal/tuple/TupleSet2.h>

namespace normal::tuple::csv {

class CSVParser {

public:

  CSVParser(std::string filePath,
			std::optional<std::vector<std::string>> columnNames,
			int64_t startOffset,
			std::optional<int64_t> finishOffset,
			int64_t bufferSize);

  CSVParser(std::string filePath,
			std::optional<std::vector<std::string>> columnNames,
			int64_t startOffset,
			std::optional<int64_t> finishOffset);

  CSVParser(const std::string &filePath,
			int64_t bufferSize);

  CSVParser(const std::string &filePath, const std::vector<std::string> &columnNames);

  explicit CSVParser(const std::string &filePath);

  /**
   * Parse a tuple set from the CSV file
   * @return
   */
  tl::expected<std::shared_ptr<TupleSet2>, std::string> parse();

  /**
   * Parse a schema from the CSV file
   * @return
   */
  tl::expected<std::shared_ptr<Schema>, std::string> parseSchema();

private:

  static constexpr int64_t DefaultBufferSize = 16 * 1024;

  std::string filePath_;
  std::optional<std::vector<std::string>> columnNames_;
  int64_t startOffset_;
  std::optional<int64_t> finishOffset_;
  int64_t bufferSize_;

  std::optional<std::shared_ptr<::arrow::io::ReadableFile>> inputStream_;

  /**
   * Opens the input stream for the file to be parsed
   *
   * @param filePath
   * @return
   */
  tl::expected<void, std::string> openInputStream();

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
   * Extracts a schema from a block parser that has parsed a single row
   *
   * @param blockParser
   * @return
   */
  static std::shared_ptr<Schema> extractSchema(const ::arrow::csv::BlockParser &blockParser);

  /**
   * Extracts string arrays from a block parser
   *
   * @param blockParser
   * @return
   */
  static tl::expected<std::vector<std::shared_ptr<::arrow::Array>>, std::string>
  extractArrays(const arrow::csv::BlockParser &blockParser);

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

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_IO_CSVPARSER_H
