//
// Created by matt on 5/3/20.
//

#ifndef FPDB_FPDB_AWS_INCLUDE_FPDB_AWS_AWSCLIENT_H
#define FPDB_FPDB_AWS_INCLUDE_FPDB_AWS_AWSCLIENT_H

#include <fpdb/aws/AWSConfig.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <memory>
#include <optional>
#include <arrow/flight/client.h>

using namespace Aws::S3;
using namespace std;

namespace fpdb::aws {

class AWSClient {

public:
  // A global AWSClient used for slave nodes in distributed node,
  // because we want all S3 operators to share the same copy of AWSClient.
  inline static shared_ptr<AWSClient> daemonClient_ = nullptr;

  AWSClient(const shared_ptr<AWSConfig> &awsConfig);

  void init();
  [[maybe_unused]] void shutdown();

  const shared_ptr<AWSConfig> &getAwsConfig() const;
  const shared_ptr<S3Client> &getS3Client() const;
  [[nodiscard]] const std::optional<std::unique_ptr<::arrow::flight::FlightClient>>& getFlightClient() const;

private:
  std::shared_ptr<S3Client> makeS3Client();

  shared_ptr<AWSConfig> awsConfig_;
  Aws::SDKOptions options_;
  shared_ptr<S3Client> s3Client_;
  std::optional<std::unique_ptr<::arrow::flight::FlightClient>> flight_client_;

};

}

#endif //FPDB_FPDB_AWS_INCLUDE_FPDB_AWS_AWSCLIENT_H
