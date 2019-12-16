//
// Created by matt on 14/12/19.
//


#include "S3SelectParser.h"
#include <arrow/csv/api.h>                              // for ReadOptions
#include <arrow/io/api.h>                              // for BufferedI...
#include <arrow/api.h>                                 // for default_m...
#include <spdlog/spdlog.h>

/**
 *
 * @param from
 * @param to
 * @return
 */
std::shared_ptr<TupleSet> S3SelectParser::parseCompletePayload(
    const std::vector<unsigned char, Aws::Allocator<unsigned char>>::iterator &from,
    const std::vector<unsigned char, Aws::Allocator<unsigned char>>::iterator &to) {

  // FIXME: Can arrow read the vector directly?
  Aws::String records(from, to);

  std::shared_ptr<arrow::io::BufferReader> reader = std::make_shared<arrow::io::BufferReader>(records);
  arrow::MemoryPool *pool = arrow::default_memory_pool();

  // FIXME: How to size the buffer?
  auto createResult = arrow::io::BufferedInputStream::Create(CSV_READER_BUFFER_SIZE, pool, reader, -1);

  if (!createResult.ok()) {
    // FIXME:
    abort();
  }

  auto input = createResult.ValueOrDie();

  auto read_options = arrow::csv::ReadOptions::Defaults();
  read_options.use_threads = false;
  read_options.autogenerate_column_names = true;
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();

  // Instantiate TableReader from input stream and options
  auto makeReaderResult = arrow::csv::TableReader::Make(pool, input, read_options,
                                                        parse_options, convert_options);
  if (!makeReaderResult.ok()) {
    // FIXME:
    abort();
  }

  std::shared_ptr<arrow::csv::TableReader> tableReader = makeReaderResult.ValueOrDie();
  std::shared_ptr<TupleSet> tupleSet = TupleSet::make(tableReader);

  return tupleSet;
}

/**
 *
 * @param payload
 * @return
 */
std::shared_ptr<TupleSet> S3SelectParser::parsePayload(Aws::Vector<unsigned char> &payload) {

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

  std::shared_ptr<TupleSet> tupleSet = S3SelectParser::parseCompletePayload(payload.begin(), payload.begin() + pos);

  return tupleSet;
}

