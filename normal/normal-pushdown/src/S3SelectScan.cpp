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

#include "normal/core/message/Message.h"                            // for Message
#include "normal/core/TupleSet.h"                           // for TupleSet
#include "s3/S3SelectParser.h"
#include <normal/core/message/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>

#include "normal/pushdown/Globals.h"

namespace Aws::Utils::RateLimits { class RateLimiterInterface; }
namespace arrow { class MemoryPool; }

using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Http;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;

namespace normal::pushdown {

void S3SelectScan::onStart() {


  //get tbl and col info
  std::string colName = m_col;
  std::string tblName = m_tbl;

  std::string cacheID = tblName + "." + colName;
  std::unordered_map<std::string, std::shared_ptr<normal::core::TupleSet>> cacheMap = m_cache->m_cacheData;
  //no found
  if (cacheMap.empty() || cacheMap.find(cacheID)==cacheMap.end()) {
    Aws::String bucketName = Aws::String(s3Bucket_);

    SelectObjectContentRequest selectObjectContentRequest;
    selectObjectContentRequest.SetBucket(bucketName);
    selectObjectContentRequest.SetKey(Aws::String(s3Object_));

    selectObjectContentRequest.SetExpressionType(ExpressionType::SQL);

    selectObjectContentRequest.SetExpression(sql_.c_str());

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

      std::shared_ptr<normal::core::message::Message> message = std::make_shared<normal::core::message::TupleMessage>(tupleSet, this->name());
      ctx()->tell(message);

      //add to cache
      if (m_cache->m_cacheData.empty() ||  m_cache->m_cacheData.find(cacheID)==cacheMap.end()){
          m_cache->m_cacheData[cacheID] = tupleSet;
      }
      else {
          m_cache->m_cacheData[cacheID] = normal::core::TupleSet::concatenate(tupleSet, m_cache->m_cacheData[cacheID]);
      }
    });
    handler.SetStatsEventCallback([&](const StatsEvent &statsEvent) {
      SPDLOG_DEBUG("Bytes scanned: {} ", statsEvent.GetDetails().GetBytesScanned());
      SPDLOG_DEBUG("Bytes processed: {}", statsEvent.GetDetails().GetBytesProcessed());
      SPDLOG_DEBUG("Bytes returned: {}", statsEvent.GetDetails().GetBytesReturned());
    });
    handler.SetEndEventCallback([&]() {
      SPDLOG_DEBUG("EndEvent:");

//      std::shared_ptr<normal::core::Message> message = std::make_shared<normal::core::CompleteMessage>();
//      ctx()->tell(message);
//
//      this->ctx()->operatorActor()->quit();

      ctx()->notifyComplete();
    });
    handler.SetOnErrorCallback([&](const AWSError<S3Errors> &errors) {
      SPDLOG_DEBUG("Error: {}", errors.GetMessage());

      // FIXME: Propagate errors here

      this->ctx()->operatorActor()->quit();
    });

    selectObjectContentRequest.SetEventStreamHandler(handler);

    auto selectObjectContentOutcome = this->s3Client_->SelectObjectContent(selectObjectContentRequest);

  }
  else {
    std::shared_ptr<normal::core::TupleSet> tupleSet = cacheMap[cacheID];
    std::shared_ptr<normal::core::message::Message> message = std::make_shared<normal::core::message::TupleMessage>(tupleSet, this->name());
    ctx()->tell(message);
//    message = std::make_shared<normal::core::CompleteMessage>();
//    ctx()->tell(message);
//    this->ctx()->operatorActor()->quit();

    ctx()->notifyComplete();
  }

}

S3SelectScan::S3SelectScan(std::string name,
                           std::string s3Bucket,
                           std::string s3Object,
                           std::string sql,
                           std::string m_tbl,
                           std::string m_col,
                           std::shared_ptr<Aws::S3::S3Client> s3Client)
    : Operator(std::move(name), "S3SelectScan"),
      s3Bucket_(std::move(s3Bucket)),
      s3Object_(std::move(s3Object)),
      sql_(std::move(sql)),
      m_cache(std::make_shared<Cache>()),
      m_col(std::move(m_col)),
      m_tbl(std::move(m_tbl)),
      s3Client_(std::move(s3Client)){
}

void S3SelectScan::onReceive(const normal::core::message::Envelope &message) {
  if (message.message().type() == "StartMessage") {
    this->onStart();
  } else {
    throw;
  }
}

}
