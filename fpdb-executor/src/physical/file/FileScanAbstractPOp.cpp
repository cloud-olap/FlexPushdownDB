//
// Created by matt on 12/12/19.
//

#include <fpdb/executor/physical/file/FileScanAbstractPOp.h>
#include <fpdb/executor/physical/file/LocalFileScanKernel.h>
#include <fpdb/executor/physical/file/RemoteFileScanKernel.h>
#include <fpdb/executor/physical/cache/CacheHelper.h>
#include <fpdb/executor/message/TransferMetricsMessage.h>
#include <fpdb/executor/metrics/Globals.h>
#include <fpdb/catalogue/local-fs/LocalFSPartition.h>
#include <fpdb/catalogue/obj-store/ObjStorePartition.h>

using namespace fpdb::executor::message;
using namespace fpdb::catalogue;

namespace fpdb::executor::physical::file {

FileScanAbstractPOp::FileScanAbstractPOp(const std::string &name,
                                         POpType type,
                                         const std::vector<std::string> &columnNames,
                                         int nodeId,
                                         const std::shared_ptr<FileScanKernel> &kernel,
                                         bool scanOnStart,
                                         bool toCache) :
	PhysicalOp(name, type, columnNames, nodeId),
	kernel_(kernel),
  scanOnStart_(scanOnStart),
  toCache_(toCache) {}

const std::shared_ptr<FileScanKernel> &FileScanAbstractPOp::getKernel() const {
  return kernel_;
}

void FileScanAbstractPOp::onReceive(const Envelope &message) {
  if (message.message().type() == MessageType::START) {
    this->onStart();
  } else if (message.message().type() == MessageType::SCAN) {
    auto scanMessage = dynamic_cast<const ScanMessage &>(message.message());
    this->onCacheLoadResponse(scanMessage);
  } else if (message.message().type() == MessageType::COMPLETE) {
    // noop
  } else {
    ctx()->notifyError("Unrecognized message type " + message.message().getTypeString());
  }
}

void FileScanAbstractPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());

  if(scanOnStart_) {
    // scan and complete
    readAndSendTuples(getProjectColumnNames());
  }
}

void FileScanAbstractPOp::onCacheLoadResponse(const ScanMessage &message) {
  auto projectColumnNames = message.getProjectColumnNames();
  if (message.isResultNeeded()) {
    readAndSendTuples(projectColumnNames);
  } else {
    // send empty tupleSet and complete first
    auto emptyTupleSet = TupleSet::makeWithEmptyTable();
    std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(emptyTupleSet, this->name());
    ctx()->tell(tupleSetMessage);
    ctx()->notifyComplete();

    // just to cache
    readTuples(projectColumnNames);
  }
}

void FileScanAbstractPOp::readAndSendTuples(const std::vector<std::string> &columnNames){
  auto readTupleSet = readTuples(columnNames);
  std::shared_ptr<Message> message = std::make_shared<TupleSetMessage>(readTupleSet, this->name());
  ctx()->tell(message);
  ctx()->notifyComplete();
}

std::shared_ptr<TupleSet> FileScanAbstractPOp::readTuples(const std::vector<std::string> &columnNames) {
  // Read the columns not present in the cache
  /*
   * TODO: support reading the file in pieces
   */

  std::shared_ptr<TupleSet> readTupleSet;
  if (columnNames.empty()) {
    readTupleSet = TupleSet::makeWithEmptyTable();
  } else {
    auto startTime = std::chrono::steady_clock::now();
    auto expectedReadTupleSet = kernel_->scan(columnNames);
    if (!expectedReadTupleSet.has_value()) {
      ctx()->notifyError(expectedReadTupleSet.error());
    }
    readTupleSet = expectedReadTupleSet.value();
    auto stopTime = std::chrono::steady_clock::now();

    // for metrics of adaptive pushdown
    if (getAdaptPushdownMetrics_) {
      auto expAdaptPushdownMetricsKey = AdaptPushdownMetricsMessage::generateAdaptPushdownMetricsKey(queryId_, name_);
      if (!expAdaptPushdownMetricsKey.has_value()) {
        ctx()->notifyError(expAdaptPushdownMetricsKey.error());
        return readTupleSet;
      }
      int64_t execTime = std::chrono::duration_cast<chrono::nanoseconds>(stopTime - startTime).count();
      std::shared_ptr<Message> adaptPushdownMetricsMessage = std::make_shared<AdaptPushdownMetricsMessage>(
              *expAdaptPushdownMetricsKey, execTime, name_);
      ctx()->notifyRoot(adaptPushdownMetricsMessage);
    }
  }

  // metrics
#if SHOW_DEBUG_METRICS == true
  std::shared_ptr<Message> execMetricsMsg;
  if (type_ == POpType::REMOTE_FILE_SCAN) {
    execMetricsMsg = std::make_shared<TransferMetricsMessage>(
            metrics::TransferMetrics(kernel_->getBytesReadRemote(), 0, 0), this->name());
  } else {
    execMetricsMsg = std::make_shared<DiskMetricsMessage>(kernel_->getBytesReadLocal(), this->name());
  }
  ctx()->notifyRoot(execMetricsMsg);
  kernel_->clearBytesRead();
#endif

  if (toCache_) {
    requestStoreSegmentsInCache(readTupleSet);
  }

  return readTupleSet;
}

void FileScanAbstractPOp::requestStoreSegmentsInCache(const std::shared_ptr<TupleSet> &tupleSet) {
  // make partition
  std::shared_ptr<Partition> partition;
  switch (kernel_->getType()) {
    case CatalogueEntryType::LOCAL_FS: {
      auto typedKernel = std::static_pointer_cast<LocalFileScanKernel>(kernel_);
      partition = std::make_shared<local_fs::LocalFSPartition>(typedKernel->getPath());
      break;
    }
    case CatalogueEntryType::OBJ_STORE: {
      auto typedKernel = std::static_pointer_cast<RemoteFileScanKernel>(kernel_);
      partition = std::make_shared<fpdb::catalogue::obj_store::ObjStorePartition>(typedKernel->getBucket(),
                                                                                  typedKernel->getObject());
      break;
    }
    default: {
      ctx()->notifyError("Unknown catalogue entry type");
    }
  }

  // make segment range
  std::pair<int64_t, int64_t> byteRange;
  auto optByteRange = kernel_->getByteRange();
  if (optByteRange.has_value()) {
    byteRange = *optByteRange;
  } else {
    byteRange = {0, kernel_->getFileSize()};
  }

  cache::CacheHelper::requestStoreSegmentsInCache(tupleSet,
                                                  partition,
                                                  byteRange.first,
                                                  byteRange.second,
                                                  name(),
                                                  ctx());
}

void FileScanAbstractPOp::clear() {
  // Noop
}

}