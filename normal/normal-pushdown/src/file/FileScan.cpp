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
#include <normal/core/cache/StoreRequestMessage.h>
#include "normal/pushdown/Globals.h"
#include <normal/tuple/csv/CSVParser.h>
#include <normal/pushdown/cache/CacheHelper.h>

using namespace normal::tuple;
using namespace normal::tuple::csv;
using namespace normal::core::cache;
using namespace normal::core::message;
using namespace normal::pushdown::cache;

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

tl::expected<std::shared_ptr<TupleSet2>, std::string> FileScan::readCSVFile(const std::vector<std::string>& columnNames) {

  CSVParser parser(filePath_, columnNames, startOffset_, finishOffset_);
  auto tupleSet = parser.parse();

  return tupleSet;
}

void FileScan::onStart() {
  SPDLOG_DEBUG("Starting");
  requestLoadSegmentsFromCache();
}

void FileScan::requestLoadSegmentsFromCache() {
  auto partition = std::make_shared<LocalFilePartition>(filePath_);
  CacheHelper::requestLoadSegmentsFromCache(columnNames_, partition, startOffset_, finishOffset_, name(), ctx());
}

void FileScan::onCacheLoadResponse(const LoadResponseMessage &Message) {

  std::vector<std::shared_ptr<Column>> columns;
  std::vector<std::shared_ptr<Column>> cachedColumns;
  std::vector<std::string> columnNamesToLoad;
  auto segments = Message.getSegments();

  auto partition = std::make_shared<LocalFilePartition>(filePath_);

  for(const auto &columnName: columnNames_){
	auto segmentKey = SegmentKey::make(partition, columnName, SegmentRange::make(startOffset_, finishOffset_));

	auto segment = segments.find(segmentKey);
	if (segment != segments.end()) {
	  cachedColumns.push_back(segment->second->getColumn());
	}
	else{
	  columnNamesToLoad.push_back(columnName);
	}
  }

  // Read the columns not present in the cache
  /*
   * FIXME: Should support reading the file in pieces
   */
  auto expectedReadTupleSet = readCSVFile(columnNamesToLoad);
  auto readTupleSet = expectedReadTupleSet.value();

  // Store the read columns in the cache
  requestStoreSegmentsInCache(readTupleSet);

  // Combine the read columns with the columns present in the cache
  columns.reserve(cachedColumns.size());
  for(const auto &column: cachedColumns){
    columns.push_back(column);
  }
  for(int c = 0;c < readTupleSet->numColumns();++c){
	columns.push_back(readTupleSet->getColumnByIndex(c).value());
  }

  auto tupleSet = TupleSet2::make(columns);

  std::shared_ptr<normal::core::message::Message> message = std::make_shared<TupleMessage>(tupleSet->toTupleSetV1(), this->name());
  ctx()->tell(message);

  ctx()->notifyComplete();
}

void FileScan::requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet) {
  auto partition = std::make_shared<LocalFilePartition>(filePath_);
  CacheHelper::requestStoreSegmentsInCache(tupleSet, partition, startOffset_, finishOffset_, name(), ctx());
}

}