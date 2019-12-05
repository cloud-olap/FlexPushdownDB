//
// Created by matt on 5/12/19.
//

#include <iostream>
#include <utility>
#include <memory>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/s3/S3Client.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/utils/crypto/Factories.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/core/Aws.h>
#include <aws/s3/model/SelectObjectContentRequest.h>
#include <aws/core/client/ClientConfiguration.h>
#include "normal/pushdown/S3SelectScan.h"


using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Http;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;

void S3SelectScan::onStart() {

    static std::string BUCKET_NAME = "s3filter";
    static const char *ALLOCATION_TAG = "Normal";
    static const char *OBJECT_KEY = "tpch-sf1/customer.csv";

    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
    Aws::InitAPI(options);

//    std::shared_ptr<S3Client> client;
//    std::shared_ptr<Aws::Utils::RateLimits::RateLimiterInterface> limiter;
//
//    limiter = Aws::MakeShared<Aws::Utils::RateLimits::DefaultRateLimiter<>>(ALLOCATION_TAG, 50000000);
//
//    ClientConfiguration config;
//    config.region = Aws::Region::US_EAST_1;
//    config.scheme = Scheme::HTTPS;
//    config.connectTimeoutMs = 30000;
//    config.requestTimeoutMs = 30000;
//    config.readRateLimiter = limiter;
//    config.writeRateLimiter = limiter;
//    config.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(ALLOCATION_TAG, 4);
//
//    client = Aws::MakeShared<S3Client>(ALLOCATION_TAG,
//                                       Aws::MakeShared<DefaultAWSCredentialsProviderChain>(ALLOCATION_TAG),
//                                       config,
//                                       AWSAuthV4Signer::PayloadSigningPolicy::Never,
//                                       true);
//
//    Aws::String bucketName = Aws::String(BUCKET_NAME);
//
//    SelectObjectContentRequest selectObjectContentRequest;
//    selectObjectContentRequest.SetBucket(bucketName);
//    selectObjectContentRequest.SetKey(Aws::String(OBJECT_KEY));
//
//    selectObjectContentRequest.SetExpressionType(ExpressionType::SQL);
//
//    selectObjectContentRequest.SetExpression("select * from S3Object s limit 100");
//
//    CSVInput csvInput;
//    csvInput.SetFileHeaderInfo(FileHeaderInfo::NONE);
//    InputSerialization inputSerialization;
//    inputSerialization.SetCSV(csvInput);
//    selectObjectContentRequest.SetInputSerialization(inputSerialization);
//
//    CSVOutput csvOutput;
//    OutputSerialization outputSerialization;
//    outputSerialization.SetCSV(csvOutput);
//    selectObjectContentRequest.SetOutputSerialization(outputSerialization);
//
//    SelectObjectContentHandler handler;
//    handler.SetRecordsEventCallback([&](const RecordsEvent &recordsEvent) {
//        auto recordsVector = recordsEvent.GetPayload();
//        Aws::String records(recordsVector.begin(), recordsVector.end());
//        std::cout << "Records event: " << records << std::endl;
//    });
//    handler.SetStatsEventCallback([&](const StatsEvent &statsEvent) {
//        std::cout << "Bytes scanned: " << statsEvent.GetDetails().GetBytesScanned() << std::endl;
//        std::cout << "Bytes processed: " << statsEvent.GetDetails().GetBytesProcessed() << std::endl;
//        std::cout << "Bytes returned: " << statsEvent.GetDetails().GetBytesReturned() << std::endl;
//    });
//
//    selectObjectContentRequest.SetEventStreamHandler(handler);
//
//    auto selectObjectContentOutcome = client->SelectObjectContent(selectObjectContentRequest);
//
//    Aws::ShutdownAPI(options);


    std::string data = "A,B,C\n1,2,3\n4,5,6\n";
    ctx()->tell(data);
}

void S3SelectScan::onStop() {
}

S3SelectScan::S3SelectScan(std::string name) : Operator(std::move(name)) {}

void S3SelectScan::onReceive(std::string msg) {
    ctx()->tell(msg);
}
