//
// Created by matt on 14/12/19.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_SRC_S3_S3SELECTEXECUTOR_H
#define NORMAL_NORMAL_PUSHDOWN_SRC_S3_S3SELECTEXECUTOR_H

#include <string>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/s3/S3Client.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/core/Aws.h>
#include <aws/s3/model/SelectObjectContentRequest.h>
#include <aws/core/client/ClientConfiguration.h>
#include <normal/tuple/TupleSet.h>

class S3SelectExecutor {
public:
  static std::shared_ptr<TupleSet> parsePayload(const Aws::String &payload);
};

#endif //NORMAL_NORMAL_PUSHDOWN_SRC_S3_S3SELECTEXECUTOR_H
