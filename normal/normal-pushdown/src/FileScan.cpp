//
// Created by matt on 12/12/19.
//

#include "normal/pushdown/FileScan.h"

#include <cstdlib>                    // for abort
#include <memory>                      // for make_unique, unique_ptr, __sha...
#include <utility>

#include <arrow/csv/options.h>         // for ReadOptions, ConvertOptions
#include <arrow/csv/reader.h>          // for TableReader
#include <arrow/io/file.h>             // for ReadableFile
#include <arrow/io/memory.h>           // for BufferReader
#include <arrow/result.h>              // for Result
#include <arrow/status.h>              // for Status
#include <arrow/type_fwd.h>            // for default_memory_pool

#include <normal/core/TupleMessage.h>
#include <normal/core/TupleSet.h>
#include "normal/core/Message.h"       // for Message
#include "normal/core/Operator.h"      // for Operator

namespace arrow { class MemoryPool; }
namespace arrow::io { class InputStream; }

FileScan::FileScan(std::string name, std::string filePath)
    : Operator(std::move(name)), m_filePath(std::move(filePath)) {}

FileScan::~FileScan() = default;

void FileScan::onStart() {

  const arrow::Result<std::shared_ptr<arrow::io::ReadableFile>> &result = arrow::io::ReadableFile::Open(m_filePath);
  if (!result.ok()) {
    abort();
  }

  const std::shared_ptr<arrow::io::ReadableFile> &f = result.ValueOrDie();
  std::shared_ptr<arrow::io::InputStream> input = arrow::io::BufferReader::GetStream(f, 0, f->GetSize().ValueOrDie());

  arrow::Status st;
  arrow::MemoryPool *pool = arrow::default_memory_pool();

  auto read_options = arrow::csv::ReadOptions::Defaults();
  read_options.use_threads = false;
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();

  // Instantiate TableReader from input stream and options
  auto makeReaderResult = arrow::csv::TableReader::Make(pool, input, read_options,
                                                        parse_options, convert_options);
  if (!makeReaderResult.ok()) {
    // Handle TableReader instantiation error...
  }

  std::shared_ptr<arrow::csv::TableReader> reader = makeReaderResult.ValueOrDie();
  std::shared_ptr<TupleSet> tupleSet = TupleSet::make(reader);

  std::unique_ptr<Message> message = std::make_unique<TupleMessage>(tupleSet);
  ctx()->tell(std::move(message));
}

void FileScan::onStop() {

}
