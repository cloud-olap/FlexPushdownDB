//
// Created by matt on 27/3/20.
//

#include <normal/pushdown/AWSClient.h>
#include "normal/sql/connector/s3/S3SelectConnector.h"

S3SelectConnector::S3SelectConnector(const std::string &name) :
    Connector(name),
    awsClient_(std::make_shared<normal::pushdown::AWSClient>()) {
}

const std::shared_ptr<normal::pushdown::AWSClient> &S3SelectConnector::getAwsClient() const {
  return awsClient_;
}
