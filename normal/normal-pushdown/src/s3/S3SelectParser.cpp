//
// Created by matt on 14/12/19.
//


#include "S3SelectParser.h"
#include <arrow/csv/api.h>                              // for ReadOptions
#include <arrow/io/api.h>                              // for BufferedI...
#include <arrow/api.h>                                 // for default_m...
#include <spdlog/spdlog.h>
#include <arrow/csv/parser.h>

namespace normal::pushdown {

/**
 *
 * @param from
 * @param to
 * @return
 */
std::shared_ptr<normal::core::TupleSet> S3SelectParser::parseCompletePayload(
    const std::vector<unsigned char, Aws::Allocator<unsigned char>>::iterator &from,
    const std::vector<unsigned char, Aws::Allocator<unsigned char>>::iterator &to) {

  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto read_options = arrow::csv::ReadOptions::Defaults();
  read_options.use_threads = false;
  read_options.autogenerate_column_names = true;
  auto convert_options = arrow::csv::ConvertOptions::Defaults();

  // FIXME: Can arrow read the vector directly?
  Aws::String records(from, to);

  auto newLineIt = std::find(from, to, '\n');
  std::basic_string_view<unsigned char> firstRow(from.base(), newLineIt - from);
  arrow::util::string_view sv = reinterpret_cast<const char *>(firstRow.data());

  arrow::csv::BlockParser p{parse_options, -1, 1};
  uint32_t out_size;
  arrow::Status status = p.Parse(sv, &out_size);
  if (!status.ok())
    abort();

  int numFields = p.num_cols();
  std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> column_types{};
  for (int i = 0; i < numFields; ++i) {
    std::stringstream ss;
    ss << "f" << i;
    column_types[ss.str()] = arrow::utf8();
  }

  convert_options.column_types = column_types;

  std::shared_ptr<arrow::io::BufferReader>
      reader = std::make_shared<arrow::io::BufferReader>(from.base(), std::distance(from, to));
  arrow::MemoryPool *pool = arrow::default_memory_pool();

  // FIXME: How to size the buffer?
  auto createResult = arrow::io::BufferedInputStream::Create(CSV_READER_BUFFER_SIZE, pool, reader, -1);

  if (!createResult.ok()) {
    // FIXME:
    abort();
  }

  auto input = createResult.ValueOrDie();



  // Instantiate TableReader from input stream and options
  auto makeReaderResult = arrow::csv::TableReader::Make(pool, input, read_options,
                                                        parse_options, convert_options);
  if (!makeReaderResult.ok()) {
    // FIXME:
    abort();
  }

  std::shared_ptr<arrow::csv::TableReader> tableReader = makeReaderResult.ValueOrDie();
  std::shared_ptr<normal::core::TupleSet> tupleSet = normal::core::TupleSet::make(tableReader);

  return tupleSet;
}

/**
 *
 * @param payload
 * @return
 */
std::shared_ptr<normal::core::TupleSet> S3SelectParser::parsePayload(Aws::Vector<unsigned char> &payload) {

  SPDLOG_TRACE({
                 Aws::String payloadString(payload.begin(), payload.end());
                 spdlog::trace("Received payload. Payload: \n{}", payloadString);
               });

  // Prepend previous partial data, if there is any

  SPDLOG_TRACE({
                 Aws::String currentPartialPayload(partial.begin(), partial.end());
                 spdlog::trace("Current partial payload: \n{}", currentPartialPayload);
               });

  if (!partial.empty()) {
    payload.insert(payload.begin(), partial.begin(), partial.end());
    partial.clear();
  }

  SPDLOG_TRACE({
                 Aws::String prependedPartialPayload(partial.begin(), partial.end());
                 spdlog::trace("Prepended partial payload: \n{}", prependedPartialPayload);
               });


  // Find the last newline
  auto newLineIt = std::find(payload.rbegin(), payload.rend(), '\n');
  int pos = (payload.rend() - newLineIt);

  // Store anything following the last newline as partial data
  partial = std::vector<unsigned char>(payload.begin() + pos, payload.end());

  SPDLOG_TRACE({
                 Aws::String newPartialPayload(partial.begin(), partial.end());
                 spdlog::trace("Partial payload: \n{}", newPartialPayload);
               });

  SPDLOG_TRACE({
                 Aws::String completePayload(payload.begin(), payload.begin() + pos);
                 spdlog::trace("Complete payload: \n{}", records3);
               });

  std::shared_ptr<normal::core::TupleSet>
      tupleSet = S3SelectParser::parseCompletePayload(payload.begin(), payload.begin() + pos);

  return tupleSet;
}

}

