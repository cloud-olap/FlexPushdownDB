//
// Created by matt on 5/12/19.
//


#include "normal/pushdown/s3/S3SelectScan.h"

#include <iostream>
#include <utility>
#include <memory>
#include <cstdlib>                                          // for abort

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/s3/S3Client.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/core/Aws.h>
#include <aws/s3/model/SelectObjectContentRequest.h>
#include <aws/core/client/ClientConfiguration.h>

#include <arrow/csv/options.h>                              // for ReadOptions
#include <arrow/csv/reader.h>                               // for TableReader
#include <arrow/io/buffered.h>                              // for BufferedI...
#include <arrow/io/memory.h>                                // for BufferReader
#include <arrow/type_fwd.h>                                 // for default_m...
#include <aws/core/Region.h>                                // for US_EAST_1
#include <aws/core/auth/AWSAuthSigner.h>                    // for AWSAuthV4...
#include <aws/core/http/Scheme.h>                           // for Scheme
#include <aws/core/utils/logging/LogLevel.h>                // for LogLevel
#include <aws/core/utils/memory/stl/AWSAllocator.h>         // for MakeShared
#include <aws/s3/model/CSVInput.h>                          // for CSVInput
#include <aws/s3/model/CSVOutput.h>                         // for CSVOutput
#include <aws/s3/model/ExpressionType.h>                    // for Expressio...
#include <aws/s3/model/FileHeaderInfo.h>                    // for FileHeade...
#include <aws/s3/model/InputSerialization.h>                // for InputSeri...
#include <aws/s3/model/OutputSerialization.h>               // for OutputSer...
#include <aws/s3/model/RecordsEvent.h>                      // for RecordsEvent
#include <aws/s3/model/SelectObjectContentHandler.h>        // for SelectObj...
#include <aws/s3/model/StatsEvent.h>                        // for StatsEvent
#include <aws/s3/model/GetObjectRequest.h>                  // for GetObj...


#include "normal/core/message/Message.h"                    // for Message
#include "normal/tuple/TupleSet.h"                          // for TupleSet
#include <normal/pushdown/TupleMessage.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include <normal/connector/s3/S3SelectPartition.h>
#include <normal/cache/SegmentKey.h>
#include <normal/connector/MiniCatalogue.h>

#include "normal/pushdown/Globals.h"
#include <normal/pushdown/cache/CacheHelper.h>

namespace Aws::Utils::RateLimits { class RateLimiterInterface; }
namespace arrow { class MemoryPool; }

using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Http;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;

using namespace normal::cache;
using namespace normal::core::cache;
using namespace normal::pushdown::cache;

namespace normal::pushdown {

S3SelectScan::S3SelectScan(std::string name,
			   std::string type,
			   std::string s3Bucket,
			   std::string s3Object,
			   std::vector<std::string> returnedS3ColumnNames,
			   std::vector<std::string> neededColumnNames,
			   int64_t startOffset,
			   int64_t finishOffset,
         std::shared_ptr<arrow::Schema> schema,
			   std::shared_ptr<Aws::S3::S3Client> s3Client,
			   bool scanOnStart,
			   bool toCache,
			   long queryId,
         std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> weightedSegmentKeys) :
	Operator(std::move(name), type, queryId),
	s3Bucket_(std::move(s3Bucket)),
	s3Object_(std::move(s3Object)),
	returnedS3ColumnNames_(std::move(returnedS3ColumnNames)),
	neededColumnNames_(std::move(neededColumnNames)),
	startOffset_(startOffset),
	finishOffset_(finishOffset),
	schema_(std::move(schema)),
	s3Client_(std::move(s3Client)),
	columnsReadFromS3_(returnedS3ColumnNames_.size()),
	scanOnStart_(scanOnStart),
	toCache_(toCache),
  weightedSegmentKeys_(std::move(weightedSegmentKeys)) {
}

void S3SelectScan::onStart() {
  SPDLOG_DEBUG("Starting");
  if (scanOnStart_) {
    readAndSendTuples();
  }
}

void S3SelectScan::readAndSendTuples() {
  auto readTupleSet = readTuples();
  SPDLOG_DEBUG("{} -> {} rows", name(), readTupleSet->numRows());
  s3SelectScanStats_.outputBytes += readTupleSet->size();
  std::shared_ptr<normal::core::message::Message>
          message = std::make_shared<TupleMessage>(readTupleSet->toTupleSetV1(), this->name());
  ctx()->tell(message);
  ctx()->notifyComplete();
}

void S3SelectScan::put(const std::shared_ptr<TupleSet2> &tupleSet) {
  auto columnNames = returnedS3ColumnNames_;

  for (int columnIndex = 0; columnIndex < tupleSet->numColumns(); ++columnIndex) {

    auto columnName = columnNames.at(columnIndex);
    auto readColumn = tupleSet->getColumnByIndex(columnIndex).value();
    auto canonicalColumnName = ColumnName::canonicalize(columnName);
    readColumn->setName(canonicalColumnName);

    auto bufferedColumnArrays = columnsReadFromS3_[columnIndex];

    if (bufferedColumnArrays == nullptr) {
      bufferedColumnArrays = std::make_shared<std::pair<std::string, ::arrow::ArrayVector>>(readColumn->getName(),
                                                                                            readColumn->getArrowArray()->chunks());
      columnsReadFromS3_[columnIndex] = bufferedColumnArrays;
    } else {
      // Add the read chunks to this buffered columns chunk vector
      for (int chunkIndex = 0; chunkIndex < readColumn->getArrowArray()->num_chunks(); ++chunkIndex) {
        auto readChunk = readColumn->getArrowArray()->chunk(chunkIndex);
        bufferedColumnArrays->second.emplace_back(readChunk);
      }
    }
  }
}


void S3SelectScan::onReceive(const normal::core::message::Envelope &message) {
  if (message.message().type() == "StartMessage") {
	  this->onStart();
  } else if (message.message().type() == "ScanMessage") {
    auto scanMessage = dynamic_cast<const scan::ScanMessage &>(message.message());
    this->onCacheLoadResponse(scanMessage);
  } else if (message.message().type() == "CompleteMessage") {
    // Noop
  } else {
    // FIXME: Propagate error properly
    throw std::runtime_error(fmt::format("Unrecognized message type: {}, {}", message.message().type(), name()));
  }
}

void S3SelectScan::onCacheLoadResponse(const scan::ScanMessage &message) {
  processScanMessage(message);

  if (message.isResultNeeded()) {
    readAndSendTuples();
  }

  else {
    auto emptyTupleSet = TupleSet2::make2();
    std::shared_ptr<normal::core::message::Message>
            responseMessage = std::make_shared<TupleMessage>(emptyTupleSet->toTupleSetV1(), this->name());
    ctx()->tell(responseMessage);
    SPDLOG_DEBUG(fmt::format("Finished because result not needed: {}/{}", s3Bucket_, s3Object_));

    /**
     * Here caching is asynchronous,
     * so need to backup ctx first, because it's a weak_ptr, after query finishing will be destroyed
     * even no use of this "ctxBackup" is ok
     */
    auto ctxBackup = ctx();

    ctx()->notifyComplete();

    // just to cache
    readTuples();
  }
}

void S3SelectScan::requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet) {
  auto partition = std::make_shared<S3SelectPartition>(s3Bucket_, s3Object_, finishOffset_ - startOffset_);
  CacheHelper::requestStoreSegmentsInCache(tupleSet, partition, startOffset_, finishOffset_, name(), ctx());
}

S3SelectScanStats S3SelectScan::getS3SelectScanStats() {
  return s3SelectScanStats_;
}

void S3SelectScan::sendSegmentWeight() {
  auto filteredBytes = (double) s3SelectScanStats_.returnedBytes;
  double projectFraction = 0.0;
  auto miniCatalogue = normal::connector::defaultMiniCatalogue;
  for (auto const &returnedColumnName: returnedS3ColumnNames_) {
    projectFraction += miniCatalogue->lengthFraction(returnedColumnName);
  }
  double totalBytes = projectFraction * ((double) s3SelectScanStats_.processedBytes);
  auto selectivity = filteredBytes / totalBytes;
  double predicateNum = getPredicateNum();

  auto weightMap = std::make_shared<std::unordered_map<std::shared_ptr<SegmentKey>, double>>();

  if (!RefinedWeightFunction) {
    /**
     * Naive weight function:
     *   w = sel * (#pred / (#pred + c))
     */
    double predPara = 0.5;
    double weight = selectivity * (predicateNum / (predicateNum + predPara));
//    double weight = selectivity * predicateNum;

    for (auto const &segmentKey: *weightedSegmentKeys_) {
      weightMap->emplace(segmentKey, weight);
    }
  }

  else {
    /**
     * Refined weight function:
     *   w = sel / vNetwork + (lenRow / (lenCol * vScan) + #pred / (lenCol * vFilter)) / #key
     */
    auto numKey = (double) weightedSegmentKeys_->size();
    for (auto const &segmentKey: *weightedSegmentKeys_) {
      auto columnName = segmentKey->getColumnName();
      auto tableName = miniCatalogue->findTableOfColumn(columnName);
      auto lenCol = (double) miniCatalogue->lengthOfColumn(columnName);
      auto lenRow = (double) miniCatalogue->lengthOfRow(tableName);

      auto weight = selectivity / vNetwork + (lenRow / (lenCol * vS3Scan) + predicateNum / (lenCol * vS3Filter)) / numKey;
//      auto weight = selectivity / vNetwork + (lenRow / (lenCol * vS3Scan)) / numKey;
      weightMap->emplace(segmentKey, weight);
    }
  }

  ctx()->send(WeightRequestMessage::make(weightMap, getQueryId(), name()), "SegmentCache")
          .map_error([](auto err) { throw std::runtime_error(err); });
}

}
