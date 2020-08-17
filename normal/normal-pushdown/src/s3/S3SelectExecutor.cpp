//
// Created by matt on 14/12/19.
//


#include "normal/pushdown/s3/S3SelectExecutor.h"
#include <arrow/csv/api.h>                              // for ReadOptions
#include <arrow/io/api.h>                              // for BufferedI...
#include <arrow/api.h>                                 // for default_m...


std::shared_ptr<TupleSet> S3SelectExecutor::parsePayload(const Aws::String &payload) {

  std::shared_ptr<arrow::io::BufferReader> reader = std::make_shared<arrow::io::BufferReader>(payload);
  arrow::MemoryPool *pool = arrow::default_memory_pool();
  auto createResult = arrow::io::BufferedInputStream::Create(128 * 1024, pool, reader, -1);

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

