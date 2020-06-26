//
// Created by matt on 5/12/19.
//


#include "normal/pushdown/s3/S3SelectScan.h"

#include <iostream>
#include <utility>
#include <memory>
#include <cstdlib>                                         // for abort

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

#include "normal/core/message/Message.h"                            // for Message
#include "normal/tuple/TupleSet.h"                           // for TupleSet
#include "S3SelectParser.h"
#include <normal/pushdown/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/core/cache/LoadRequestMessage.h>
#include <normal/core/cache/LoadResponseMessage.h>
#include <normal/connector/s3/S3SelectPartition.h>
#include <normal/cache/SegmentKey.h>

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
						   std::string s3Bucket,
						   std::string s3Object,
						   std::string sql,
						   std::vector<std::string> columnNames,
						   int64_t startOffset,
						   int64_t finishOffset,
						   S3SelectCSVParseOptions parseOptions,
						   std::shared_ptr<Aws::S3::S3Client> s3Client) :
	Operator(std::move(name), "S3SelectScan"),
	s3Bucket_(std::move(s3Bucket)),
	s3Object_(std::move(s3Object)),
	sql_(std::move(sql)),
	columnNames_(std::move(columnNames)),
	startOffset_(startOffset),
	finishOffset_(finishOffset),
	parseOptions_(parseOptions),
	s3Client_(std::move(s3Client)),
	columns_(columnNames_.size()) {
}

std::shared_ptr<S3SelectScan> S3SelectScan::make(std::string name,
												 std::string s3Bucket,
												 std::string s3Object,
												 std::string sql,
												 std::vector<std::string> columnNames,
												 int64_t startOffset,
												 int64_t finishOffset,
												 S3SelectCSVParseOptions parseOptions,
												 std::shared_ptr<Aws::S3::S3Client> s3Client) {
  return std::make_shared<S3SelectScan>(name,
										s3Bucket,
										s3Object,
										sql,
										columnNames,
										startOffset,
										finishOffset,
										parseOptions,
										s3Client);

}

tl::expected<void, std::string> S3SelectScan::s3Select(const TupleSetEventCallback &tupleSetEventCallback) {

  std::optional<std::string> optionalErrorMessage;

  Aws::String bucketName = Aws::String(s3Bucket_);

  SelectObjectContentRequest selectObjectContentRequest;
  selectObjectContentRequest.SetBucket(bucketName);
  selectObjectContentRequest.SetKey(Aws::String(s3Object_));

  ScanRange scanRange;
  scanRange.SetStart(startOffset_);
  scanRange.SetEnd(finishOffset_);

  selectObjectContentRequest.SetScanRange(scanRange);

  selectObjectContentRequest.SetExpressionType(ExpressionType::SQL);

  selectObjectContentRequest.SetExpression(sql_.c_str());
//  std::string pullUpSql = "select ";
//  for (auto colName:columnNamesToLoad){
//      pullUpSql += colName + ", ";
//  }
//  pullUpSql.pop_back();
//  pullUpSql.pop_back();
//  pullUpSql += " from s3Object";
//  if (pushDownFlag_) {
//      selectObjectContentRequest.SetExpression(sql_.c_str());
//  }
//  else {
//      selectObjectContentRequest.SetExpression(pullUpSql.c_str());
//  }

  CSVInput csvInput;
  csvInput.SetFileHeaderInfo(FileHeaderInfo::USE);
  csvInput.SetFieldDelimiter(parseOptions_.getFieldDelimiter().c_str());
  csvInput.SetRecordDelimiter(parseOptions_.getRecordDelimiter().c_str());
  InputSerialization inputSerialization;
  inputSerialization.SetCSV(csvInput);
  selectObjectContentRequest.SetInputSerialization(inputSerialization);

  CSVOutput csvOutput;
  OutputSerialization outputSerialization;
  outputSerialization.SetCSV(csvOutput);
  selectObjectContentRequest.SetOutputSerialization(outputSerialization);

  std::vector<unsigned char> partial{};
  S3SelectParser s3SelectParser{};

  SelectObjectContentHandler handler;
  handler.SetRecordsEventCallback([&](const RecordsEvent &recordsEvent) {
	SPDLOG_DEBUG("S3 Select RecordsEvent  |  partition: s3://{}/{}, size: {}",
				 s3Bucket_,
				 s3Object_,
				 recordsEvent.GetPayload().size());
	auto payload = recordsEvent.GetPayload();
	std::shared_ptr<TupleSet> tupleSetV1 = s3SelectParser.parsePayload(payload);
	auto tupleSet = TupleSet2::create(tupleSetV1);
	tupleSetEventCallback(tupleSet);
  });
  handler.SetStatsEventCallback([&](const StatsEvent &statsEvent) {
	SPDLOG_DEBUG("S3 Select StatsEvent  |  partition: s3://{}/{}, scanned: {}, processed: {}, returned: {}",
				 s3Bucket_,
				 s3Object_,
				 statsEvent.GetDetails().GetBytesScanned(),
				 statsEvent.GetDetails().GetBytesProcessed(),
				 statsEvent.GetDetails().GetBytesReturned());
  });
  handler.SetEndEventCallback([&]() {
	SPDLOG_DEBUG("S3 Select EndEvent  |  partition: s3://{}/{}",
				 s3Bucket_,
				 s3Object_);
  });
  handler.SetOnErrorCallback([&](const AWSError<S3Errors> &errors) {
	SPDLOG_DEBUG("S3 Select Error  |  partition: s3://{}/{}, message: {}",
				 s3Bucket_,
				 s3Object_,
				 std::string(errors.GetMessage()));
	optionalErrorMessage = std::optional(errors.GetMessage());
  });

  selectObjectContentRequest.SetEventStreamHandler(handler);

  auto selectObjectContentOutcome = this->s3Client_->SelectObjectContent(selectObjectContentRequest);

  if (optionalErrorMessage.has_value()) {
	return tl::unexpected(optionalErrorMessage.value());
  } else {
	return {};
  }
}

void S3SelectScan::onStart() {
  SPDLOG_DEBUG("Starting");
  requestLoadSegmentsFromCache();
}

void S3SelectScan::requestLoadSegmentsFromCache() {
  auto partition = std::make_shared<S3SelectPartition>(s3Bucket_, s3Object_);
  CacheHelper::requestLoadSegmentsFromCache(columnNames_, partition, startOffset_, finishOffset_, name(), ctx());
}

void S3SelectScan::onCacheLoadResponse(const LoadResponseMessage &Message) {

  std::vector<std::shared_ptr<Column>> columns;
  std::vector<std::shared_ptr<Column>> cachedColumns;
  std::vector<std::string> columnNamesToLoad;

  auto cachedSegments = Message.getSegments();

  // Gather the columns that were in the cache plus the columns we need to load
  auto partition = std::make_shared<S3SelectPartition>(s3Bucket_, s3Object_);
  for (const auto &columnName: columnNames_) {
	auto segmentKey = SegmentKey::make(partition, columnName, SegmentRange::make(startOffset_, finishOffset_));

	auto segment = cachedSegments.find(segmentKey);
	if (segment != cachedSegments.end()) {
	  cachedColumns.push_back(segment->second->getColumn());
	} else {
	  columnNamesToLoad.push_back(columnName);
	}
  }

  SPDLOG_DEBUG("Reading From S3");

  // Read the columns not present in the cache
  auto result = s3Select([&](const std::shared_ptr<TupleSet2> &tupleSet) {

	for (int columnIndex = 0; columnIndex < tupleSet->numColumns(); ++columnIndex) {

	  auto columnName = columnNames_.at(columnIndex);
	  auto readColumn = tupleSet->getColumnByIndex(columnIndex).value();
	  auto canonicalColumnName = ColumnName::canonicalize(columnName);
	  readColumn->setName(canonicalColumnName);

	  auto bufferedColumnArrays = columns_[columnIndex];

	  if (bufferedColumnArrays == nullptr) {
		bufferedColumnArrays = std::make_shared<std::pair<std::string, ::arrow::ArrayVector>>(readColumn->getName(), readColumn->getArrowArray()->chunks());
		columns_[columnIndex] = bufferedColumnArrays;
	  } else {
		// Add the read chunks to this buffered columns chunk vector
		for (int chunkIndex = 0; chunkIndex < readColumn->getArrowArray()->num_chunks(); ++chunkIndex) {
		  auto readChunk = readColumn->getArrowArray()->chunk(chunkIndex);
		  bufferedColumnArrays->second.emplace_back(readChunk);
		}
	  }
	}
  });

  if (!result.has_value()) {
	throw std::runtime_error(result.error());
  }

  std::vector<std::shared_ptr<Column>> readColumns;
  for(const auto& arrays: columns_){
	readColumns.emplace_back(Column::make(arrays->first, arrays->second));
  }

  auto readTupleSet = TupleSet2::make(readColumns);

  // Store the read columns in the cache
  requestStoreSegmentsInCache(readTupleSet);

  // Combine the read columns with the columns that were loaded from the cache
  columns.reserve(cachedColumns.size());
  for (const auto &column: cachedColumns) {
	columns.push_back(column);
  }
  for (const auto &readColumn: readColumns) {
	columns.push_back(readColumn);
  }

  auto completeTupleSet = TupleSet2::make(columns);

  std::shared_ptr<normal::core::message::Message>
	  message = std::make_shared<TupleMessage>(completeTupleSet->toTupleSetV1(), this->name());
  ctx()->tell(message);

  SPDLOG_DEBUG("Finished Reading");

  ctx()->notifyComplete();
}

void S3SelectScan::onTuple(const core::message::TupleMessage &message) {
  std::shared_ptr<normal::core::message::Message>
	  tupleMessage = std::make_shared<normal::core::message::TupleMessage>(message.tuples(), this->name());
  ctx()->tell(tupleMessage);
}

void S3SelectScan::onReceive(const normal::core::message::Envelope &message) {
  if (message.message().type() == "StartMessage") {
	this->onStart();
  } else if (message.message().type() == "TupleMessage") {
	auto tupleMessage = dynamic_cast<const normal::core::message::TupleMessage &>(message.message());
	this->onTuple(tupleMessage);
  } else if (message.message().type() == "LoadResponseMessage") {
	auto loadResponseMessage = dynamic_cast<const LoadResponseMessage &>(message.message());
	this->onCacheLoadResponse(loadResponseMessage);
  } else if (message.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(message.message());
	onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + message.message().type());
  }
}

void S3SelectScan::onComplete(const normal::core::message::CompleteMessage &) {
  ctx()->notifyComplete();
}

void S3SelectScan::requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet) {
  auto partition = std::make_shared<S3SelectPartition>(s3Bucket_, s3Object_);
  CacheHelper::requestStoreSegmentsInCache(tupleSet, partition, startOffset_, finishOffset_, name(), ctx());
}

}
