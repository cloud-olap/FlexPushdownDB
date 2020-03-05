//
// Created by matt on 5/12/19.
//


#include "normal/pushdown/S3SelectScan.h"

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

#include "normal/core/Message.h"                            // for Message
#include "normal/core/TupleSet.h"                           // for TupleSet
#include "s3/S3SelectParser.h"
#include <normal/core/TupleMessage.h>

#include "normal/pushdown/Globals.h"

namespace Aws::Utils::RateLimits { class RateLimiterInterface; }
namespace arrow { class MemoryPool; }

using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Http;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;

void S3SelectScan::onStart() {

  static const char *ALLOCATION_TAG = "Normal";

  Aws::SDKOptions options;
  options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
  Aws::InitAPI(options);

  std::shared_ptr<S3Client> client;
  std::shared_ptr<Aws::Utils::RateLimits::RateLimiterInterface> limiter;

  limiter = Aws::MakeShared<Aws::Utils::RateLimits::DefaultRateLimiter<>>(ALLOCATION_TAG, 50000000);

  ClientConfiguration config;
  config.region = Aws::Region::US_EAST_1;
  config.scheme = Scheme::HTTPS;
  config.connectTimeoutMs = 30000;
  config.requestTimeoutMs = 30000;
  config.readRateLimiter = limiter;
  config.writeRateLimiter = limiter;
  config.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(ALLOCATION_TAG, 4);

  client = Aws::MakeShared<S3Client>(ALLOCATION_TAG,
                                     Aws::MakeShared<DefaultAWSCredentialsProviderChain>(ALLOCATION_TAG),
                                     config,
                                     AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                     true);

  Aws::String bucketName = Aws::String(m_s3Bucket);

  SelectObjectContentRequest selectObjectContentRequest;
  selectObjectContentRequest.SetBucket(bucketName);
  selectObjectContentRequest.SetKey(Aws::String(m_s3Object));

  selectObjectContentRequest.SetExpressionType(ExpressionType::SQL);

  selectObjectContentRequest.SetExpression(m_sql.c_str());

  CSVInput csvInput;
  csvInput.SetFileHeaderInfo(FileHeaderInfo::USE);
  csvInput.SetFieldDelimiter("|");
  csvInput.SetRecordDelimiter("|\n");
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
    auto payload = recordsEvent.GetPayload();
    std::shared_ptr<normal::core::TupleSet> tupleSet = s3SelectParser.parsePayload(payload);

    std::shared_ptr<normal::core::Message> message = std::make_shared<normal::core::TupleMessage> (tupleSet);
    ctx()->tell(message);
  });
  handler.SetStatsEventCallback([&](const StatsEvent &statsEvent) {
    SPDLOG_DEBUG("Bytes scanned: {} ", statsEvent.GetDetails().GetBytesScanned());
    SPDLOG_DEBUG("Bytes processed: {}", statsEvent.GetDetails().GetBytesProcessed());
    SPDLOG_DEBUG("Bytes returned: {}", statsEvent.GetDetails().GetBytesReturned());
  });
  handler.SetEndEventCallback([&](){
    ctx()->complete();
  });

  selectObjectContentRequest.SetEventStreamHandler(handler);

  auto selectObjectContentOutcome = client->SelectObjectContent(selectObjectContentRequest);

  Aws::ShutdownAPI(options);

}

S3SelectScan::S3SelectScan(std::string name, std::string s3Bucket, std::string s3Object, std::string sql)
    : Operator(std::move(name)) {
  m_s3Bucket = std::move(s3Bucket);
  m_s3Object = std::move(s3Object);
  m_sql = std::move(sql);
}
