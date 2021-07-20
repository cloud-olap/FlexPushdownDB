//
// Created by matt on 5/3/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_AWSCLIENT_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_AWSCLIENT_H

#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/s3/S3Client.h>

namespace normal::pushdown {
class AWSClient {
private:
  Aws::SDKOptions options_;
public:
  void init();
  [[maybe_unused]] void shutdown();
  static std::shared_ptr<Aws::S3::S3Client> defaultS3Client();
};
inline bool initialized_ = false;
}
#endif //NORMAL_NORMAL_PUSHDOWN_SRC_AWSCLIENT_H
