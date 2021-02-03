//
// Created by Matt Woicik on 1/19/21.
//

#include "normal/pushdown/s3/S3Select.h"

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

S3Select::S3Select(std::string name,
						   std::string s3Bucket,
						   std::string s3Object,
						   std::string filterSql,
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
  S3SelectScan(std::move(name), "S3Select", std::move(s3Bucket), std::move(s3Object),
               std::move(returnedS3ColumnNames), std::move(neededColumnNames),
               startOffset, finishOffset, std::move(schema),
               std::move(s3Client), scanOnStart, toCache, queryId, std::move(weightedSegmentKeys)),
	filterSql_(std::move(filterSql)) {
  generateParser();
}

std::shared_ptr<S3Select> S3Select::make(const std::string& name,
												 const std::string& s3Bucket,
												 const std::string& s3Object,
												 const std::string& filterSql,
												 const std::vector<std::string>& returnedS3ColumnNames,
												 const std::vector<std::string>& neededColumnNames,
												 int64_t startOffset,
												 int64_t finishOffset,
                         const std::shared_ptr<arrow::Schema>& schema,
												 const std::shared_ptr<Aws::S3::S3Client>& s3Client,
												 bool scanOnstart,
												 bool toCache,
												 long queryId,
                         const std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>>& weightedSegmentKeys) {
  return std::make_shared<S3Select>(name,
										s3Bucket,
										s3Object,
										filterSql,
										returnedS3ColumnNames,
										neededColumnNames,
										startOffset,
										finishOffset,
										schema,
										s3Client,
										scanOnstart,
										toCache,
										queryId,
										weightedSegmentKeys);

}

void S3Select::generateParser() {
  std::vector<std::shared_ptr<::arrow::Field>> fields;
  for (auto const &columnName: returnedS3ColumnNames_) {
    fields.emplace_back(schema_->GetFieldByName(columnName));
  }
  parser_ = S3CSVParser::make(returnedS3ColumnNames_, std::make_shared<::arrow::Schema>(fields));
}

InputSerialization S3Select::getInputSerialization() {
  InputSerialization inputSerialization;
  if (s3Object_.find("csv") != std::string::npos) {
    CSVInput csvInput;
    csvInput.SetFileHeaderInfo(FileHeaderInfo::USE);
    // This is the standard field delimiter and record delimiter for S3 Select, so it is hardcoded here
    csvInput.SetFieldDelimiter(",");
    csvInput.SetRecordDelimiter("\n");
    inputSerialization.SetCSV(csvInput);
    if (s3Object_.find("gz") != std::string::npos) {
      inputSerialization.SetCompressionType(CompressionType::GZIP);
    } else if (s3Object_.find("bz2") != std::string::npos) {
      inputSerialization.SetCompressionType(CompressionType::BZIP2);
    }
  } else if (s3Object_.find("parquet") != std::string::npos) {
    ParquetInput parquetInput;
    inputSerialization.SetParquet(parquetInput);
  }
  return inputSerialization;
}

bool S3Select::scanRangeSupported() {
  if (s3Object_.find("gz") != std::string::npos ||
      s3Object_.find("bz2") != std::string::npos) {
    return false;
  }
  return true;
}

tl::expected<void, std::string> S3Select::s3Select() {

  // s3select request
  std::optional<std::string> optionalErrorMessage;

  Aws::String bucketName = Aws::String(s3Bucket_);

  SelectObjectContentRequest selectObjectContentRequest;
  selectObjectContentRequest.SetBucket(bucketName);
  selectObjectContentRequest.SetKey(Aws::String(s3Object_));

//  if (scanRangeSupported()) {
//    ScanRange scanRange;
//    scanRange.SetStart(startOffset_);
//    scanRange.SetEnd(finishOffset_);
//
//    selectObjectContentRequest.SetScanRange(scanRange);
//  }
  selectObjectContentRequest.SetExpressionType(ExpressionType::SQL);

  // combine columns with filterSql
  std::string sql = "";
  for (auto colIndex = 0; colIndex < neededColumnNames_.size(); colIndex++) {
    if (colIndex == 0) {
      sql += neededColumnNames_[colIndex];
    } else {
      sql += ", " + neededColumnNames_[colIndex];
    }
  }
  sql = "select " + sql + " from s3Object" + filterSql_;

  selectObjectContentRequest.SetExpression(sql.c_str());

  InputSerialization inputSerialization = getInputSerialization();

  selectObjectContentRequest.SetInputSerialization(inputSerialization);

  CSVOutput csvOutput;
  OutputSerialization outputSerialization;
  outputSerialization.SetCSV(csvOutput);
  selectObjectContentRequest.SetOutputSerialization(outputSerialization);

  SelectObjectContentHandler handler;
  handler.SetRecordsEventCallback([&](const RecordsEvent &recordsEvent) {
	SPDLOG_DEBUG("S3 Select RecordsEvent  |  name: {}, size: {}",
				 name(),
				 recordsEvent.GetPayload().size());
	auto payload = recordsEvent.GetPayload();
	if (payload.size() > 0) {
	  // Airmettle doesn't trigger StatsEvent callback, so add up returned bytes here.
	  returnedBytes_ += payload.size();
    std::chrono::steady_clock::time_point startConversionTime = std::chrono::steady_clock::now();
    std::shared_ptr<TupleSet> tupleSetV1 = parser_->parsePayload(payload);
    auto tupleSet = TupleSet2::create(tupleSetV1);
    put(tupleSet);
    std::chrono::steady_clock::time_point stopConversionTime = std::chrono::steady_clock::now();
    auto conversionTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
            stopConversionTime - startConversionTime).count();
    selectConvertTimeNS_ += conversionTime;
  }
  });
  handler.SetStatsEventCallback([&](const StatsEvent &statsEvent) {
	SPDLOG_DEBUG("S3 Select StatsEvent  |  name: {}, scanned: {}, processed: {}, returned: {}",
				 name(),
				 statsEvent.GetDetails().GetBytesScanned(),
				 statsEvent.GetDetails().GetBytesProcessed(),
				 statsEvent.GetDetails().GetBytesReturned());
	processedBytes_ += statsEvent.GetDetails().GetBytesProcessed();
//	returnedBytes_ += statsEvent.GetDetails().GetBytesReturned();
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

  std::chrono::steady_clock::time_point startTransferConvertTime = std::chrono::steady_clock::now();
  auto selectObjectContentOutcome = s3Client_->SelectObjectContent(selectObjectContentRequest);
  std::chrono::steady_clock::time_point stopTransferConvertTime = std::chrono::steady_clock::now();
  selectTransferAndConvertNS_ = std::chrono::duration_cast<std::chrono::nanoseconds>(stopTransferConvertTime - startTransferConvertTime).count();
  numRequests_++;

  if (optionalErrorMessage.has_value()) {
	return tl::unexpected(optionalErrorMessage.value());
  } else {
	return {};
  }
}

std::shared_ptr<TupleSet2> S3Select::readTuples() {
  std::shared_ptr<TupleSet2> readTupleSet;

  if (returnedS3ColumnNames_.empty()) {
    readTupleSet = TupleSet2::make2();
  } else {

    SPDLOG_DEBUG("Reading From S3: {}", name());

    // Read columns from s3
    auto result = s3Select();

    if (!result.has_value()) {
      throw std::runtime_error(fmt::format("{}, {}", result.error(), name()));
    }

    std::vector<std::shared_ptr<Column>> readColumns;

    for (int col_id = 0; col_id < returnedS3ColumnNames_.size(); col_id++) {
      auto &arrays = columnsReadFromS3_.at(col_id);
      if (arrays) {
        readColumns.emplace_back(Column::make(arrays->first, arrays->second));
      } else {
        // Make empty column according to schema_
        auto columnName = returnedS3ColumnNames_.at(col_id);
        readColumns.emplace_back(Column::make(columnName, schema_->GetFieldByName(columnName)->type()));
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

  SPDLOG_DEBUG("Finished Reading: {}", name());
  return readTupleSet;
}

void S3Select::processScanMessage(const scan::ScanMessage &message) {
  // This is for hybrid caching as we later determine which columns to pull up
  returnedS3ColumnNames_ = message.getColumnNames();
  neededColumnNames_ = message.getColumnNames();
  columnsReadFromS3_ = std::vector<std::shared_ptr<std::pair<std::string, ::arrow::ArrayVector>>>(returnedS3ColumnNames_.size());
  // Need to update parser as we have modified the columns we are reading from S3
  generateParser();
};

int subStrNum(const std::string& str, const std::string& sub) {
  int num = 0;
  size_t len = sub.length();
  if (len == 0) len = 1;
  for (size_t i = 0; (i = str.find(sub, i)) != std::string::npos; num++, i += len);
  return num;
}

int S3Select::getPredicateNum() {
  // FIXME: Unsure if this +1 at the end, left it as it was originally present but unsure if this is necessary
  //        This +1 makes it so an empty filterSql_ string has one predicate. Is that intended? Perhaps it is fine
  //        if we are counting project.
  return subStrNum(filterSql_, "and") + subStrNum(filterSql_, "or") + 1;
}

}
