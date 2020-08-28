//
// Created by matt on 5/3/20.
//

#include "normal/pushdown/AWSClient.h"

#include <aws/core/Aws.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/s3/S3Client.h>
namespace normal::pushdown {
void AWSClient::init() {
  options_.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
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
  std::shared_ptr<Aws::Utils::RateLimits::RateLimiterInterface> limiter;

  limiter = Aws::MakeShared<Aws::Utils::RateLimits::DefaultRateLimiter<>>(ALLOCATION_TAG, 500000000);

  Aws::Client::ClientConfiguration config;
  config.region = Aws::Region::US_EAST_1;
  config.scheme = Aws::Http::Scheme::HTTP;
  config.connectTimeoutMs = 30000;
  config.requestTimeoutMs = 30000;
  config.readRateLimiter = limiter;
  config.writeRateLimiter = limiter;
  config.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(ALLOCATION_TAG, 4);

  s3Client = Aws::MakeShared<Aws::S3::S3Client>(ALLOCATION_TAG,
                                              Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>(
                                                  ALLOCATION_TAG),
                                              config,
                                              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                              true);

  return s3Client;
}
}