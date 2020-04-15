//
// Created by matt on 7/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_S3SELECTSCANLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_S3SELECTSCANLOGICALOPERATOR_H

#include <memory>
#include <string>

#include <normal/core/Operator.h>
#include <normal/pushdown/AWSClient.h>

#include <normal/sql/logical/ScanLogicalOperator.h>

namespace normal::sql::logical {

class S3SelectScanLogicalOperator: public ScanLogicalOperator {
private:
  std::string s3Bucket_;
  std::string s3Object_;
  std::shared_ptr<pushdown::AWSClient> awsClient_;

public:
  S3SelectScanLogicalOperator(std::string S3Bucket,
                              std::string S3Object,
                              std::shared_ptr<pushdown::AWSClient> AwsClient);

  std::shared_ptr<core::Operator> toOperator() override;
};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_S3SELECTSCANLOGICALOPERATOR_H
