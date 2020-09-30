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

FileScan::FileScan(std::string name, const std::string& filePath, long queryId) :
	Operator(std::move(name), "FileScan"),
	queryId_(queryId),
	scanOnStart_(true),
	kernel_(FileScanKernel::make(filePath, FileType::CSV, 0, ULONG_MAX)){}

FileScan::FileScan(std::string name,
				   const std::string& filePath,
				   FileType fileType,
				   std::vector<std::string>  columnNames,
				   unsigned long startOffset,
				   unsigned long finishOffset,
				   long queryId,
				   bool scanOnStart) :
	Operator(std::move(name), "FileScan"),
	queryId_(queryId),
	scanOnStart_(scanOnStart),
	columnNames_(std::move(columnNames)),
	kernel_(FileScanKernel::make(filePath, fileType, startOffset, finishOffset)){}

std::shared_ptr<FileScan> FileScan::make(const std::string& name,
										 const std::string& filePath,
										 const std::vector<std::string>& columnNames,
										 unsigned long startOffset,
										 unsigned long finishOffset, long queryId,
										 bool scanOnStart) {

  auto canonicalColumnNames = ColumnName::canonicalize(columnNames);

  return std::make_shared<FileScan>(name,
									filePath,
									FileType::CSV,
									canonicalColumnNames,
									startOffset,
									finishOffset,
									queryId,
									scanOnStart);
}

std::shared_ptr<FileScan> FileScan::make(const std::string& name,
										 const std::string& filePath,
										 FileType fileType,
										 const std::vector<std::string>& columnNames,
										 unsigned long startOffset,
										 unsigned long finishOffset, long queryId,
										 bool scanOnStart) {

  auto canonicalColumnNames = ColumnName::canonicalize(columnNames);

  return std::make_shared<FileScan>(name,
									filePath,
									fileType,
									canonicalColumnNames,
									startOffset,
									finishOffset,
									queryId,
									scanOnStart);
}

void FileScan::onReceive(const Envelope &message) {
  if (message.message().type() == "StartMessage") {
	this->onStart();
  } else if (message.message().type() == "ScanMessage") {
	auto scanMessage = dynamic_cast<const ScanMessage &>(message.message());
	this->onCacheLoadResponse(scanMessage);
  }
  else if (message.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(message.message());
	this->onComplete(completeMessage);
  }
  else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + message.message().type());
  }
}

void FileScan::onComplete(const normal::core::message::CompleteMessage &) {
  if(ctx()->operatorMap().allComplete(OperatorRelationshipType::Producer)){
	ctx()->notifyComplete();
  }
}

void FileScan::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
  if(scanOnStart_){
	readAndSendTuples(columnNames_);
	ctx()->notifyComplete();
  }
}

void FileScan::readAndSendTuples(const std::vector<std::string> &columnNames){
  // Read the columns not present in the cache
  /*
   * FIXME: Should support reading the file in pieces
   */

  std::shared_ptr<TupleSet2> readTupleSet;
  if (columnNames.empty()) {
    readTupleSet = TupleSet2::make2();
  } else {
    auto expectedReadTupleSet = kernel_->scan(columnNames);
    readTupleSet = expectedReadTupleSet.value();

    // Store the read columns in the cache
    requestStoreSegmentsInCache(readTupleSet);
  }

  std::shared_ptr<normal::core::message::Message> message = std::make_shared<TupleMessage>(readTupleSet->toTupleSetV1(), this->name());
  ctx()->tell(message);
}

void FileScan::onCacheLoadResponse(const ScanMessage &Message) {
  readAndSendTuples(Message.getColumnNames());
}

void FileScan::requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet) {
  auto partition = std::make_shared<LocalFilePartition>(kernel_->getPath());
  CacheHelper::requestStoreSegmentsInCache(tupleSet, partition, kernel_->getStartPos(), kernel_->getFinishPos(), name(), ctx());
}
long FileScan::getQueryId() const {
  return queryId_;
}
bool FileScan::isScanOnStart() const {
  return scanOnStart_;
}
const std::vector<std::string> &FileScan::getColumnNames() const {
  return columnNames_;
}
const std::unique_ptr<FileScanKernel> &FileScan::getKernel() const {
  return kernel_;
}

}