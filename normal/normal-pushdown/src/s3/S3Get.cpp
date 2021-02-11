//
// Created by Matt Woicik on 1/19/21.
//

#include "normal/pushdown/s3/S3Get.h"

#include <iostream>
#include <utility>
#include <memory>
#include <cstdlib>                                          // for abort

#include "zlib.h"
#include "zconf.h"

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
  // Put this logic in S3CSVParser
  std::string csvString(std::istreambuf_iterator<char>(retrievedFile), {});
  processedBytes_ += csvString.size();
  returnedBytes_ += csvString.size();


  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto read_options = arrow::csv::ReadOptions::Defaults();
  read_options.use_threads = false;
  read_options.skip_rows = 1; // Skip the header
  read_options.column_names = returnedS3ColumnNames_;
  auto convert_options = arrow::csv::ConvertOptions::Defaults();
  std::unordered_map<std::string, std::shared_ptr<::arrow::DataType>> columnTypes;
  for(const auto &columnName: returnedS3ColumnNames_){
	  columnTypes.emplace(columnName, schema_->GetFieldByName(columnName)->type());
  }
  convert_options.column_types = columnTypes;

  // Create a reader
  auto reader = std::make_shared<arrow::io::BufferReader>(csvString);
  // Instantiate TableReader from input stream and options
  auto makeReaderResult = arrow::csv::TableReader::Make(arrow::default_memory_pool(),
														reader,
														read_options,
														parse_options,
														convert_options);
  if (!makeReaderResult.ok())
	throw std::runtime_error(fmt::format(
		"Cannot parse S3 payload  |  Could not create a table reader, error: '{}'",
		makeReaderResult.status().message()));
  auto tableReader = *makeReaderResult;

  // Parse the payload and create the tupleset
  auto tupleSetV1 = TupleSet::make(tableReader);
  auto tupleSet = TupleSet2::create(tupleSetV1);
  put(tupleSet);
}

// Currently this deflates at ~60MB/s
bool gzipInflate( const std::string& compressedBytes, std::string& uncompressedBytes ) {
  if ( compressedBytes.size() == 0 ) {
    uncompressedBytes = compressedBytes ;
    return true ;
  }

  unsigned half_length = compressedBytes.size() / 2;

  unsigned uncompLength = compressedBytes.size() * 3;
  // GZIP compression reduces size by ~1/3 so estimate allocating this much at the start
  char* uncomp = (char*) calloc( sizeof(char), uncompLength);

  z_stream strm;
  strm.next_in = (Bytef *) compressedBytes.c_str();
  strm.avail_in = compressedBytes.size() ;
  strm.total_out = 0;
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;

  bool done = false ;

  if (inflateInit2(&strm, (16+MAX_WBITS)) != Z_OK) {
    free( uncomp );
    return false;
  }

  while (!done) {
    // If our output buffer is too small
    if (strm.total_out >= uncompLength ) {
      // Increase size of output buffer
      char* uncomp2 = (char*) calloc( sizeof(char), uncompLength + half_length );
      memcpy( uncomp2, uncomp, uncompLength );
      uncompLength += half_length ;
      free( uncomp );
      uncomp = uncomp2 ;
    }

    strm.next_out = (Bytef *) (uncomp + strm.total_out);
    strm.avail_out = uncompLength - strm.total_out;

    // Inflate another chunk.
    int err = inflate (&strm, Z_SYNC_FLUSH);
    if (err == Z_STREAM_END) done = true;
    else if (err != Z_OK)  {
      break;
    }
  }

  if (inflateEnd (&strm) != Z_OK) {
    free( uncomp );
    return false;
  }

  uncompressedBytes = std::string(uncomp);
  free( uncomp );
  return true ;
}

void S3Get::readGZIPCSVFile(std::basic_iostream<char, std::char_traits<char>> &retrievedFile) {
// TODO: Get this working with boost, as boost has support for gzip and bzip2 which will simplify this.
//       It is also likely optimized more than the above code and so it should be faster.
//       This is probably not necessary for now as Airmettle doesn't have full support for compressed data.
//
//  example using boost:
//  std::stringstream decompressed;
//
//  boost::iostreams::filtering_streambuf<boost::iostreams::input> out;
//  out.push(boost::iostreams::gzip_decompressor());
//  out.push(compressed);
//  boost::iostreams::copy(out, decompressed);
  std::string decompressedData;
  std::string gzipString(std::istreambuf_iterator<char>(retrievedFile), {});
  // From offline comparisons this is correct or at least approximately correct
  processedBytes_ += gzipString.size();
  returnedBytes_ += gzipString.size();
  if (!gzipInflate(gzipString, decompressedData)) {
    SPDLOG_ERROR("Error deflating gzip file from S3 for {}", name());
  }

  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto read_options = arrow::csv::ReadOptions::Defaults();
  read_options.use_threads = false;
  read_options.skip_rows = 1; // Skip the header
  read_options.column_names = returnedS3ColumnNames_;
  auto convert_options = arrow::csv::ConvertOptions::Defaults();
  std::unordered_map<std::string, std::shared_ptr<::arrow::DataType>> columnTypes;
  for(const auto &columnName: returnedS3ColumnNames_){
	  columnTypes.emplace(columnName, schema_->GetFieldByName(columnName)->type());
  }
  convert_options.column_types = columnTypes;

  // Create a reader
  auto reader = std::make_shared<arrow::io::BufferReader>(decompressedData);

  // Instantiate TableReader from input stream and options
  auto makeReaderResult = arrow::csv::TableReader::Make(arrow::default_memory_pool(),
														reader,
														read_options,
														parse_options,
														convert_options);
  if (!makeReaderResult.ok())
	throw std::runtime_error(fmt::format(
		"Cannot parse S3 payload  |  Could not create a table reader, error: '{}'",
		makeReaderResult.status().message()));
  auto tableReader = *makeReaderResult;

  // Parse the payload and create the tupleset
  auto tupleSetV1 = TupleSet::make(tableReader);
  auto tupleSet = TupleSet2::create(tupleSetV1);
  put(tupleSet);
}

void S3Get::readParquetFile(std::basic_iostream<char, std::char_traits<char>> &retrievedFile) {
  std::string parquetFileString(std::istreambuf_iterator<char>(retrievedFile), {});
  // From offline comparisons this is correct or at least approximately correct
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
}

tl::expected<void, std::string> S3Get::s3Get() {
  GetObjectRequest getObjectRequest;
  getObjectRequest.SetBucket(Aws::String(s3Bucket_));
  getObjectRequest.SetKey(Aws::String(s3Object_));

  std::chrono::steady_clock::time_point startTransferTime = std::chrono::steady_clock::now();
  GetObjectOutcome getObjectOutcome = s3Client_->GetObject(getObjectRequest);
  std::chrono::steady_clock::time_point stopTransferTime = std::chrono::steady_clock::now();
  auto transferTime = std::chrono::duration_cast<std::chrono::nanoseconds>(stopTransferTime - startTransferTime).count();
  getTransferTimeNS_ += transferTime;
  numRequests_++;

  if (getObjectOutcome.IsSuccess()) {
    std::chrono::steady_clock::time_point startConversionTime = std::chrono::steady_clock::now();
    auto &retrievedFile = getObjectOutcome.GetResultWithOwnership().GetBody();
    if (s3Object_.find("csv") != std::string::npos) {
      if (s3Object_.find("gz") != std::string::npos) {
        readGZIPCSVFile(retrievedFile);
      } else {
        readCSVFile(retrievedFile);
      }
    } else { // (s3Object_.find("parquet") != std::string::npos)
      readParquetFile(retrievedFile);
    }
    std::chrono::steady_clock::time_point stopConversionTime = std::chrono::steady_clock::now();
    auto conversionTime = std::chrono::duration_cast<std::chrono::nanoseconds>(stopConversionTime - startConversionTime).count();
    getConvertTimeNS_ += conversionTime;

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
