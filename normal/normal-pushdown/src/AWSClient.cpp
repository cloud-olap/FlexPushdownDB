//
// Created by matt on 5/3/20.
//

#include "normal/pushdown/AWSClient.h"

#include <aws/core/Aws.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/s3/S3Client.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <normal/plan/Globals.h>
#include <normal/pushdown/Globals.h>

namespace normal::pushdown {

void AWSClient::init() {
  options_.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
  Aws::InitAPI(options_);
}

void AWSClient::shutdown() {
  Aws::ShutdownAPI(options_);
}

std::shared_ptr<Aws::S3::S3Client> AWSClient::defaultS3Client() {
  auto client = std::make_shared<AWSClient>();
  client->init();

  static const char *ALLOCATION_TAG = "Normal";

  std::shared_ptr<Aws::S3::S3Client> s3Client;

  Aws::Client::ClientConfiguration config;
  config.region = Aws::Region::US_WEST_1;
  config.scheme = Aws::Http::Scheme::HTTP;
  config.maxConnections = 200; // Default = 25
  config.retryStrategy = Aws::MakeShared<Aws::Client::DefaultRetryStrategy>(ALLOCATION_TAG, 0, 0); // Disable retries
  config.connectTimeoutMs = 500000;
  config.requestTimeoutMs = 900000;
  // Default is to create and dispatch to thread with async methods, we don't use async so default is ideal
  config.executor = Aws::MakeShared<Aws::Utils::Threading::DefaultExecutor>(ALLOCATION_TAG);
  config.verifySSL = false;

  // Commented this out as turning it off increase transfer rate. It appears that having this set
  // reduces transfer rate even if the chosen value is very high.
  if (normal::pushdown::NetworkLimit > 0) {
    std::shared_ptr<Aws::Utils::RateLimits::RateLimiterInterface> limiter;
    limiter = Aws::MakeShared<Aws::Utils::RateLimits::DefaultRateLimiter<>>(ALLOCATION_TAG, normal::pushdown::NetworkLimit);
    config.readRateLimiter = limiter;
    config.writeRateLimiter = limiter;
  }

  if (normal::plan::useAirmettle) {
    SPDLOG_INFO("Using Airmettle Client");
    config.endpointOverride = "54.219.101.189:80/s3/test";
    Aws::String accessKeyId = "test-test";
    Aws::String secretKey = "test";
    Aws::Auth::AWSCredentials airmettleCredentials = Aws::Auth::AWSCredentials(accessKeyId, secretKey);

    s3Client = Aws::MakeShared<Aws::S3::S3Client>(
      ALLOCATION_TAG,
      airmettleCredentials,
      config,
      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
      false);
  } else {
    SPDLOG_INFO("Using S3 Client");
    s3Client = Aws::MakeShared<Aws::S3::S3Client>(
      ALLOCATION_TAG,
      Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>(ALLOCATION_TAG),
      config,
      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
      true);
  }

  return s3Client;
}

}
