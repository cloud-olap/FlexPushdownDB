//
// Created by matt on 7/4/20.
//

#include <normal/sql/logical/S3SelectScanLogicalOperator.h>

#include <normal/pushdown/S3SelectScan.h>

normal::sql::logical::S3SelectScanLogicalOperator::S3SelectScanLogicalOperator(std::string S3Bucket,
                                                                               std::string S3Object,
                                                                               std::shared_ptr<normal::pushdown::AWSClient> AwsClient)
    : s3Bucket_(std::move(S3Bucket)),
      s3Object_(std::move(S3Object)),
      awsClient_(std::move(AwsClient)) {}

std::shared_ptr<normal::core::Operator> normal::sql::logical::S3SelectScanLogicalOperator::toOperator() {
  return std::make_shared<normal::pushdown::S3SelectScan>("s3scan",
                                                          this->s3Bucket_,
                                                          this->s3Object_,
                                                          "select * from S3Object",
                                                          this->s3Object_,
                                                          "A",
                                                          this->awsClient_->defaultS3Client());
}

