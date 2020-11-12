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
						   std::string s3Bucket,
						   std::string s3Object,
						   std::string filterSql,
						   std::vector<std::string> columnNames,
						   int64_t startOffset,
						   int64_t finishOffset,
               std::shared_ptr<arrow::Schema> schema,
						   S3SelectCSVParseOptions parseOptions,
						   std::shared_ptr<Aws::S3::S3Client> s3Client,
						   bool scanOnStart,
               bool toCache,
               long queryId,
               std::pair<bool, std::vector<std::string>> enablePushdownProject,
               const std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> &weightedSegmentKeys) :
	Operator(std::move(name), "S3SelectScan", queryId),
	s3Bucket_(std::move(s3Bucket)),
	s3Object_(std::move(s3Object)),
	filterSql_(std::move(filterSql)),
	columnNames_(std::move(columnNames)),
	startOffset_(startOffset),
	finishOffset_(finishOffset),
	schema_(std::move(schema)),
	parseOptions_(parseOptions),
	s3Client_(std::move(s3Client)),
	columns_(enablePushdownProject.first ? columnNames_.size() : enablePushdownProject.second.size()),
	scanOnStart_(scanOnStart),
	toCache_(toCache),
	enablePushdownProject_(enablePushdownProject),
  weightedSegmentKeys_(weightedSegmentKeys) {
}

std::shared_ptr<S3SelectScan> S3SelectScan::make(std::string name,
												 std::string s3Bucket,
												 std::string s3Object,
												 std::string filterSql,
												 std::vector<std::string> columnNames,
												 int64_t startOffset,
												 int64_t finishOffset,
                         std::shared_ptr<arrow::Schema> schema,
												 S3SelectCSVParseOptions parseOptions,
												 std::shared_ptr<Aws::S3::S3Client> s3Client,
												 bool scanOnstart,
												 bool toCache,
												 long queryId,
                         std::pair<bool, std::vector<std::string>> enablePushdownProject,
                         std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> weightedSegmentKeys) {
  return std::make_shared<S3SelectScan>(name,
										s3Bucket,
										s3Object,
										filterSql,
										columnNames,
										startOffset,
										finishOffset,
										schema,
										parseOptions,
										s3Client,
										scanOnstart,
										toCache,
										queryId,
										enablePushdownProject,
										weightedSegmentKeys);

}

void S3SelectScan::generateParser() {
  // Generate parser according to columns to fetch
  if (enablePushdownProject_.first) {
    std::vector<std::shared_ptr<::arrow::Field>> fields;
    for (auto const &columnName: columnNames_) {
      fields.emplace_back(schema_->GetFieldByName(columnName));
    }
    parser_ = S3SelectParser::make(columnNames_, std::make_shared<::arrow::Schema>(fields));
  } else {
    parser_ = S3SelectParser::make(enablePushdownProject_.second, schema_);
  }
}

tl::expected<void, std::string> S3SelectScan::s3Select() {

  // s3select request
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

  // combine columns with filterSql
  auto columnNames = enablePushdownProject_.first ? columnNames_ : enablePushdownProject_.second;
  std::string sql = "";
  for (auto colIndex = 0; colIndex < columnNames.size(); colIndex++) {
    if (colIndex == 0) {
      sql += columnNames[colIndex];
    } else {
      sql += ", " + columnNames[colIndex];
    }
  }
  sql = "select " + sql + " from s3Object" + filterSql_;

  selectObjectContentRequest.SetExpression(sql.c_str());

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
  S3SelectParser s3SelectParser({}, {});

  SelectObjectContentHandler handler;
  handler.SetRecordsEventCallback([&](const RecordsEvent &recordsEvent) {
	SPDLOG_DEBUG("S3 Select RecordsEvent  |  name: {}, size: {}",
				 name(),
				 recordsEvent.GetPayload().size());
	auto payload = recordsEvent.GetPayload();
	std::shared_ptr<TupleSet> tupleSetV1 = parser_->parsePayload(payload);
	auto tupleSet = TupleSet2::create(tupleSetV1);
	put(tupleSet);
  });
  handler.SetStatsEventCallback([&](const StatsEvent &statsEvent) {
	SPDLOG_DEBUG("S3 Select StatsEvent  |  name: {}, scanned: {}, processed: {}, returned: {}",
				 name(),
				 statsEvent.GetDetails().GetBytesScanned(),
				 statsEvent.GetDetails().GetBytesProcessed(),
				 statsEvent.GetDetails().GetBytesReturned());
	processedBytes_ += statsEvent.GetDetails().GetBytesProcessed();
	returnedBytes_ += statsEvent.GetDetails().GetBytesReturned();
  });
  handler.SetEndEventCallback([&]() {
	SPDLOG_DEBUG("S3 Select EndEvent  |  name: {}",
				 name());
  });
  handler.SetOnErrorCallback([&](const AWSError<S3Errors> &errors) {
	SPDLOG_DEBUG("S3 Select Error  |  name: {}, message: {}",
				 name(),
				 std::string(errors.GetMessage()));
	optionalErrorMessage = std::optional(errors.GetMessage());
  });

  selectObjectContentRequest.SetEventStreamHandler(handler);

  auto selectObjectContentOutcome = this->s3Client_->SelectObjectContent(selectObjectContentRequest);
  numRequests_++;

  if (optionalErrorMessage.has_value()) {
	return tl::unexpected(optionalErrorMessage.value());
  } else {
	return {};
  }
}

tl::expected<void, std::string> S3SelectScan::s3Scan() {
  GetObjectRequest getObjectRequest;
  getObjectRequest.SetBucket(Aws::String(s3Bucket_));
  getObjectRequest.SetKey(Aws::String(s3Object_));

  GetObjectOutcome getObjectOutcome = this->s3Client_->GetObject(getObjectRequest);
  numRequests_++;

  if (getObjectOutcome.IsSuccess()) {
    auto &retrievedFile = getObjectOutcome.GetResultWithOwnership().GetBody();
    S3SelectParser s3SelectParser({}, {});
    auto readSize = DefaultS3ScanBufferSize - 1;
    char buffer[DefaultS3ScanBufferSize];

    // Discard column names
    SPDLOG_DEBUG("S3 Scan starts  |  name: {}", name());
    retrievedFile.getline(buffer, readSize);

    // Get content
    while (!retrievedFile.eof()) {
      memset(buffer, 0, DefaultS3ScanBufferSize);

      SPDLOG_DEBUG("S3 Scan buffer  |  name: {}, numBytes: {}", name(), readSize);
      retrievedFile.read(buffer, readSize);
      processedBytes_ += strlen(buffer);
      returnedBytes_ += strlen(buffer);
      Aws::Vector<unsigned char> charAwsVec(buffer, buffer + readSize);

      std::shared_ptr<TupleSet> tupleSetV1 = parser_->parsePayload(charAwsVec);
      auto tupleSet = TupleSet2::create(tupleSetV1);
      put(tupleSet);
    }

    return {};
  }

  else {
    const auto& err = getObjectOutcome.GetError();
    return tl::unexpected(err.GetMessage().c_str());
  }
}

void S3SelectScan::onStart() {
  SPDLOG_DEBUG("Starting");
  if (scanOnStart_) {
    readAndSendTuples();
  }
}

void S3SelectScan::readAndSendTuples() {
  auto readTupleSet = readTuples();
  std::shared_ptr<normal::core::message::Message>
          message = std::make_shared<TupleMessage>(readTupleSet->toTupleSetV1(), this->name());
  ctx()->tell(message);
  ctx()->notifyComplete();
}

void S3SelectScan::put(const std::shared_ptr<TupleSet2> &tupleSet) {
  auto columnNames = enablePushdownProject_.first ? columnNames_ : enablePushdownProject_.second;

  for (int columnIndex = 0; columnIndex < tupleSet->numColumns(); ++columnIndex) {

    auto columnName = columnNames.at(columnIndex);
    auto readColumn = tupleSet->getColumnByIndex(columnIndex).value();
    auto canonicalColumnName = ColumnName::canonicalize(columnName);
    readColumn->setName(canonicalColumnName);

    auto bufferedColumnArrays = columns_[columnIndex];

    if (bufferedColumnArrays == nullptr) {
      bufferedColumnArrays = std::make_shared<std::pair<std::string, ::arrow::ArrayVector>>(readColumn->getName(),
                                                                                            readColumn->getArrowArray()->chunks());
      columns_[columnIndex] = bufferedColumnArrays;
    } else {
      // Add the read chunks to this buffered columns chunk vector
      for (int chunkIndex = 0; chunkIndex < readColumn->getArrowArray()->num_chunks(); ++chunkIndex) {
        auto readChunk = readColumn->getArrowArray()->chunk(chunkIndex);
        bufferedColumnArrays->second.emplace_back(readChunk);
      }
    }
  }
}

std::shared_ptr<TupleSet2> S3SelectScan::readTuples() {
  std::shared_ptr<TupleSet2> readTupleSet;

  if (columnNames_.empty()) {
    readTupleSet = TupleSet2::make2();
  } else {

    SPDLOG_DEBUG(fmt::format("Reading From S3: {}", name()));

    // Generate parser
    generateParser();

    // Read columns from s3
    auto result = enablePushdownProject_.first ? s3Select() : s3Scan();

    if (!result.has_value()) {
      throw std::runtime_error(fmt::format("{}, {}", result.error(), name()));
    }

    std::vector<std::shared_ptr<Column>> readColumns;

    if (enablePushdownProject_.first) {
      for (int col_id = 0; col_id < columns_.size(); col_id++) {
        auto &arrays = columns_.at(col_id);
        if (arrays) {
          readColumns.emplace_back(Column::make(arrays->first, arrays->second));
        } else {
          // Make empty column according to schema_
          auto columnName = columnNames_.at(col_id);
          readColumns.emplace_back(Column::make(columnName, schema_->GetFieldByName(columnName)->type()));
        }
      }
    }

    else {
      // An implicit projection on the entire object to get needed columns
      for (auto const &columnName: columnNames_) {
        // Find arrays using column names
        std::shared_ptr<std::pair<std::string, ::arrow::ArrayVector>> arrays = nullptr;
        for (auto const &columnPair: columns_) {
          if (!columnPair) {
            // no tuples
            break;
          }
          if (columnName == columnPair->first) {
            arrays = columnPair;
            break;
          }
        }
        if (arrays) {
          readColumns.emplace_back(Column::make(arrays->first, arrays->second));
        } else {
          // Make empty column according to schema_
          readColumns.emplace_back(Column::make(columnName, schema_->GetFieldByName(columnName)->type()));
        }
      }
    }

    readTupleSet = TupleSet2::make(readColumns);

    // Store the read columns in the cache, if not in full-pushdown mode
    if (toCache_) {
      // TODO: only send caching columns
      requestStoreSegmentsInCache(readTupleSet);
    } else {
      // send segment filter weight
      if (weightedSegmentKeys_ && processedBytes_ > 0) {
        sendSegmentWeight();
      }
    }
  }

  SPDLOG_DEBUG(fmt::format("Finished Reading: {}", name()));
  return readTupleSet;
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
  columnNames_ = message.getColumnNames();
  columns_ = std::vector<std::shared_ptr<std::pair<std::string, ::arrow::ArrayVector>>>(enablePushdownProject_.first ? columnNames_.size() : enablePushdownProject_.second.size());

  if (message.isResultNeeded()) {
    readAndSendTuples();
  }

  else {
    auto emptyTupleSet = TupleSet2::make2();
    std::shared_ptr<normal::core::message::Message>
            message = std::make_shared<TupleMessage>(emptyTupleSet->toTupleSetV1(), this->name());
    ctx()->tell(message);
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

size_t S3SelectScan::getProcessedBytes() const {
  return processedBytes_;
}

size_t S3SelectScan::getReturnedBytes() const {
  return returnedBytes_;
}

size_t S3SelectScan::getNumRequests() const {
  return numRequests_;
}

int subStrNum(const std::string& str, const std::string& sub) {
  int num = 0;
  size_t len = sub.length();
  if (len == 0) len = 1;
  for (size_t i = 0; (i = str.find(sub, i)) != std::string::npos; num++, i += len);
  return num;
}

int getPredicateNum(std::string &filterSql) {
  return subStrNum(filterSql, "and") + subStrNum(filterSql, "or") + 1;
}

void S3SelectScan::sendSegmentWeight() {
  auto filteredBytes = (double) returnedBytes_;
  double projectFraction = 0.0;
  auto miniCatalogue = normal::connector::defaultMiniCatalogue;
  for (auto const &columnName: columnNames_) {
    projectFraction += miniCatalogue->lengthFraction(columnName);
  }
  double totalBytes = projectFraction * ((double) processedBytes_);
  auto selectivity = filteredBytes / totalBytes;
  double predicateNum = (double) getPredicateNum(filterSql_);

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

//      auto weight = selectivity / vNetwork + (lenRow / (lenCol * vS3Scan) + predicateNum / (lenCol * vS3Filter)) / numKey;
      auto weight = selectivity / vNetwork + (lenRow / (lenCol * vS3Scan)) / numKey;
      weightMap->emplace(segmentKey, weight);
    }
  }

  ctx()->send(WeightRequestMessage::make(weightMap, getQueryId(), name()), "SegmentCache")
          .map_error([](auto err) { throw std::runtime_error(err); });
}

}
