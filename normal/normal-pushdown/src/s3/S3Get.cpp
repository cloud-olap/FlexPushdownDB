//
// Created by Matt Woicik on 1/19/21.
//

#include "normal/pushdown/s3/S3Get.h"

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
#include <parquet/arrow/reader.h>

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
S3Get::S3Get(std::string name,
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
  S3SelectScan(std::move(name), "S3Get", std::move(s3Bucket), std::move(s3Object),
               std::move(returnedS3ColumnNames), std::move(neededColumnNames),
               startOffset, finishOffset, std::move(schema),
               std::move(s3Client), scanOnStart, toCache, queryId, std::move(weightedSegmentKeys)) {
}

std::shared_ptr<S3Get> S3Get::make(const std::string& name,
												 const std::string& s3Bucket,
												 const std::string& s3Object,
												 const std::vector<std::string>& returnedS3ColumnNames,
												 const std::vector<std::string>& neededColumnNames,
												 int64_t startOffset,
												 int64_t finishOffset,
                         const std::shared_ptr<arrow::Schema>& schema,
												 const std::shared_ptr<Aws::S3::S3Client>& s3Client,
												 bool scanOnStart,
												 bool toCache,
												 long queryId,
                         const std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>>& weightedSegmentKeys) {
  return std::make_shared<S3Get>(name,
										s3Bucket,
										s3Object,
										returnedS3ColumnNames,
										neededColumnNames,
										startOffset,
										finishOffset,
										schema,
										s3Client,
										scanOnStart,
										toCache,
										queryId,
										weightedSegmentKeys);

}

void S3Get::readCSVFile(std::basic_iostream<char, std::char_traits<char>> &retrievedFile) {
  std::chrono::steady_clock::time_point startConversionTime = std::chrono::steady_clock::now();
  auto readSize = DefaultS3ScanBufferSize - 1;
  char buffer[DefaultS3ScanBufferSize];

  auto parser = S3CSVParser::make(returnedS3ColumnNames_, schema_);

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

    std::shared_ptr<TupleSet> tupleSetV1 = parser->parsePayload(charAwsVec);
    auto tupleSet = TupleSet2::create(tupleSetV1);
    put(tupleSet);
  }
  std::chrono::steady_clock::time_point stopConversionTime = std::chrono::steady_clock::now();
  auto conversionTime = std::chrono::duration_cast<std::chrono::nanoseconds>(stopConversionTime - startConversionTime).count();
  getConvertTimeNS_ += conversionTime;
}

void S3Get::readParquetFile(std::basic_iostream<char, std::char_traits<char>> &retrievedFile) {
  std::chrono::steady_clock::time_point startConversionTime = std::chrono::steady_clock::now();
  std::string parquetFileString(std::istreambuf_iterator<char>(retrievedFile), {});
  // From offline calculations this seems approximately correct
  processedBytes_ += parquetFileString.size();
  returnedBytes_ += parquetFileString.size();
  auto bufferedReader = std::make_shared<arrow::io::BufferReader>(parquetFileString);

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  arrow::MemoryPool *pool = arrow::default_memory_pool();
  arrow::Status st = parquet::arrow::OpenFile(bufferedReader, pool, &arrow_reader);

  if (!st.ok()) {
    SPDLOG_ERROR("Error opening file for {}\nError: {}", name(), st.message());
  }
  std::shared_ptr<arrow::Table> table;
  st = arrow_reader->ReadTable(&table);
  if (!st.ok()) {
    SPDLOG_ERROR("Error reading parquet data for {}\nError: {}", name(), st.message());
  }
  auto tupleSetV1 = TupleSet::make(table);
  auto tupleSet = TupleSet2::create(tupleSetV1);
  put(tupleSet);
  std::chrono::steady_clock::time_point stopConversionTime = std::chrono::steady_clock::now();
  auto conversionTime = std::chrono::duration_cast<std::chrono::nanoseconds>(stopConversionTime - startConversionTime).count();
  getConvertTimeNS_ += conversionTime;
}

tl::expected<void, std::string> S3Get::s3Get() {
  GetObjectRequest getObjectRequest;
  getObjectRequest.SetBucket(Aws::String(s3Bucket_));
  getObjectRequest.SetKey(Aws::String(s3Object_));

  std::chrono::steady_clock::time_point startTransferTime = std::chrono::steady_clock::now();
  GetObjectOutcome getObjectOutcome = this->s3Client_->GetObject(getObjectRequest);
  std::chrono::steady_clock::time_point stopTransferTime = std::chrono::steady_clock::now();
  auto transferTime = std::chrono::duration_cast<std::chrono::nanoseconds>(stopTransferTime - startTransferTime).count();
  getTransferTimeNS_ += transferTime;
  numRequests_++;

  if (getObjectOutcome.IsSuccess()) {
    auto &retrievedFile = getObjectOutcome.GetResultWithOwnership().GetBody();
    if (s3Object_.find("csv") != std::string::npos) {
      readCSVFile(retrievedFile);
    } else { // (s3Object_.find("parquet") != std::string::npos)
      readParquetFile(retrievedFile);
    }

    return {};
  }

  else {
    const auto& err = getObjectOutcome.GetError();
    return tl::unexpected(err.GetMessage().c_str());
  }
}

std::shared_ptr<TupleSet2> S3Get::readTuples() {
  std::shared_ptr<TupleSet2> readTupleSet;

  if (returnedS3ColumnNames_.empty()) {
    readTupleSet = TupleSet2::make2();
  } else {

    SPDLOG_DEBUG("Reading From S3: {}", name());

    // Read columns from s3
    auto result = s3Get();

    if (!result.has_value()) {
      throw std::runtime_error(fmt::format("{}, {}", result.error(), name()));
    }

    std::vector<std::shared_ptr<Column>> readColumns;

    // An implicit projection on the entire object to get needed columns
    for (auto const &columnName: neededColumnNames_) {
      // Find arrays using column names
      std::shared_ptr<std::pair<std::string, ::arrow::ArrayVector>> arrays = nullptr;
      for (auto const &columnPair: columnsReadFromS3_) {
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

void S3Get::processScanMessage(const scan::ScanMessage &message) {
  // This is for hybrid caching as we later determine which columns to pull up
  // Though currently this is only called for SELECT in our system
  neededColumnNames_ = message.getColumnNames();
};

int S3Get::getPredicateNum() {
  return 0;
}

}
