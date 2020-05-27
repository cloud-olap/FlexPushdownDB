//
// Created by matt on 12/12/19.
//

#include "normal/pushdown/file/FileScan.h"

#include <cstdlib>                    // for abort
#include <memory>                      // for make_unique, unique_ptr, __sha...
#include <utility>

#include <arrow/csv/options.h>         // for ReadOptions, ConvertOptions
#include <arrow/csv/reader.h>          // for TableReader
#include <arrow/io/file.h>             // for ReadableFile
#include <arrow/status.h>              // for Status
#include <arrow/type_fwd.h>            // for default_memory_pool
#include <arrow/csv/parser.h>

#include <normal/pushdown/TupleMessage.h>
#include <normal/tuple/TupleSet.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/connector/local-fs/LocalFilePartition.h>

#include "normal/core/message/Message.h"       // for Message
#include "normal/core/Operator.h"      // for Operator
#include <normal/cache/SegmentKey.h>
#include <normal/core/cache/LoadRequestMessage.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include "normal/pushdown/Globals.h"
#include <normal/tuple/csv/CSVParser.h>

using namespace normal::tuple;
using namespace normal::tuple::csv;
using namespace normal::core::cache;
using namespace normal::core::message;

namespace arrow { class MemoryPool; }

namespace normal::pushdown {

FileScan::FileScan(std::string name, std::string filePath) :
	Operator(std::move(name), "FileScan"),
	filePath_(std::move(filePath)),
	startOffset_(0),
	finishOffset_(ULONG_MAX) {}

FileScan::FileScan(std::string name,
				   std::string filePath,
				   std::vector<std::string> columnNames,
				   unsigned long startOffset,
				   unsigned long finishOffset) :
	Operator(std::move(name), "FileScan"),
	filePath_(std::move(filePath)),
	columnNames_(std::move(columnNames)),
	startOffset_(startOffset),
	finishOffset_(finishOffset) {}

std::shared_ptr<FileScan> FileScan::make(std::string name,
										 std::string filePath,
										 std::vector<std::string> columnNames,
										 unsigned long startOffset,
										 unsigned long finishOffset) {

  auto canonicalColumnNames = ColumnName::canonicalize(columnNames);

  return std::make_shared<FileScan>(name,
									filePath,
									canonicalColumnNames,
									startOffset,
									finishOffset);
}

void FileScan::onReceive(const Envelope &message) {
  if (message.message().type() == "StartMessage") {
	this->onStart();
  } else if (message.message().type() == "LoadResponseMessage") {
	auto loadResponseMessage = dynamic_cast<const LoadResponseMessage &>(message.message());
	this->onCacheLoadResponse(loadResponseMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + message.message().type());
  }
}

tl::expected<std::shared_ptr<TupleSet>, std::string> FileScan::readCSVFile() {

  auto pool = arrow::default_memory_pool();

  auto res = arrow::io::ReadableFile::Open(filePath_);
  if (!res.ok())
	return tl::unexpected(res.status().message());

  auto input = res.ValueOrDie();

  CSVParser parser(filePath_);
  auto schema = parser.parseSchema();
  auto fields = schema.value()->fields();

  std::vector<std::string> fieldNames;
  for (const auto &field: fields) {
	fieldNames.push_back(field->name());
  }

  auto st = input->Seek(startOffset_);
  if (!st.ok())
	return tl::unexpected(st.message());

  auto parseOptions = arrow::csv::ParseOptions::Defaults();
  auto readOptions = arrow::csv::ReadOptions::Defaults();
  readOptions.use_threads = false;
  readOptions.column_names = fieldNames;
  readOptions.skip_rows = 1;
  auto convertOptions = arrow::csv::ConvertOptions::Defaults();
  std::unordered_map<std::string, std::shared_ptr<::arrow::DataType>> columnTypes;
  for(const auto &field: fields){
	columnTypes[field->name()] = field->type();
  }
  convertOptions.column_types = columnTypes;
  auto reader = arrow::csv::TableReader::Make(pool,
											  input,
											  readOptions,
											  parseOptions,
											  convertOptions).ValueOrDie();

  auto tupleSet = TupleSet::make(reader);

  return tupleSet;
}

void FileScan::onStart() {
  requestCachedSegment();
}

void FileScan::requestCachedSegment() {
  auto partition1 = std::make_shared<LocalFilePartition>(filePath_);
  auto segmentKey1 = SegmentKey::make(partition1, SegmentRange::make(startOffset_, finishOffset_));

  ctx()->send(LoadRequestMessage::make(segmentKey1, name()), "SegmentCache")
	  .map_error([](auto err) { throw std::runtime_error(err); });
}

void FileScan::onCacheLoadResponse(const LoadResponseMessage &Message) {

  std::shared_ptr<TupleSet> tupleSet;
  if (Message.getSegmentData().has_value()) {
	tupleSet = Message.getSegmentData().value()->getTupleSet()->toTupleSetV1();
  } else {
	tupleSet = readCSVFile()
		.map_error([](auto err) { throw std::runtime_error(err); }).value();
  }

  std::shared_ptr<normal::core::message::Message> message = std::make_shared<TupleMessage>(tupleSet, this->name());
  ctx()->tell(message);

  ctx()->notifyComplete();
}

}