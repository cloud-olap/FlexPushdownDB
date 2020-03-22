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
#include <arrow/status.h>              // for Status
#include <arrow/type_fwd.h>            // for default_memory_pool

#include <normal/core/TupleMessage.h>
#include <normal/core/TupleSet.h>
#include <normal/core/CompleteMessage.h>

#include "normal/core/Message.h"       // for Message
#include "normal/core/Operator.h"      // for Operator
#include "io/CSVParser.h"

#include "normal/pushdown/Globals.h"

namespace arrow { class MemoryPool; }

namespace normal::pushdown {

FileScan::FileScan(std::string name, std::string filePath)
    : Operator(std::move(name)), filePath_(std::move(filePath)) {}

FileScan::~FileScan() = default;

void FileScan::onReceive(const normal::core::Envelope &message) {
  if (message.message().type() == "StartMessage") {
    this->onStart();
  } else {
    throw;
  }
}

void FileScan::onStart() {

  SPDLOG_DEBUG("Starting");

  arrow::Status st;
  auto pool = arrow::default_memory_pool();

  auto input = arrow::io::ReadableFile::Open(filePath_).ValueOrDie();

  auto fields = CSVParser::readFields(input);

  st = input->Seek(0);
  if (!st.ok())
    abort();

  auto parseOptions = arrow::csv::ParseOptions::Defaults();
  auto readOptions = arrow::csv::ReadOptions::Defaults();
  readOptions.use_threads = false;
  auto convertOptions = arrow::csv::ConvertOptions::Defaults();

  auto reader = arrow::csv::TableReader::Make(pool,
                                              input,
                                              readOptions, parseOptions, convertOptions).ValueOrDie();

  auto tupleSet = normal::core::TupleSet::make(reader);

  std::shared_ptr<normal::core::Message> message = std::make_shared<normal::core::TupleMessage>(tupleSet);
  ctx()->tell(message);

  SPDLOG_DEBUG("Completing");
  std::shared_ptr<normal::core::Message> cm = std::make_shared<normal::core::CompleteMessage>();
  ctx()->tell(cm);

  ctx()->operatorActor()->quit();
}

}