//
// Created by Yifei Yang on 11/10/21.
//

#ifndef FPDB_FPDB_AWS_INCLUDE_FPDB_AWS_AWSCONFIG_H
#define FPDB_FPDB_AWS_INCLUDE_FPDB_AWS_AWSCONFIG_H

#include <fpdb/aws/S3ClientType.h>
#include <memory>

using namespace std;

namespace fpdb::aws {

class AWSConfig {
public:
  AWSConfig(S3ClientType s3ClientType = S3ClientType::S3,
            size_t networkLimit = 0);

  static shared_ptr<AWSConfig> parseAWSConfig();

  S3ClientType getS3ClientType() const;
  size_t getNetworkLimit() const;

private:
  static S3ClientType parseS3ClientType(const string& stringToParse);

  S3ClientType s3ClientType_;
  size_t networkLimit_;
};

}


#endif //FPDB_FPDB_AWS_INCLUDE_FPDB_AWS_AWSCONFIG_H
