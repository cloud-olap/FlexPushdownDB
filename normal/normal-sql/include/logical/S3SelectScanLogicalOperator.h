//
// Created by matt on 7/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_S3SELECTSCANLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_SRC_AST_S3SELECTSCANLOGICALOPERATOR_H

#include <normal/pushdown/AWSClient.h>
#include "logical/ScanLogicalOperator.h"

class S3SelectScanLogicalOperator: public ScanLogicalOperator {
private:
  std::string s3Bucket_;
  std::string s3Object_;
  std::shared_ptr<normal::pushdown::AWSClient> awsClient_;

public:
  S3SelectScanLogicalOperator(std::string S3Bucket,
                              std::string S3Object,
                              std::shared_ptr<normal::pushdown::AWSClient> AwsClient);

  std::shared_ptr<normal::core::Operator> toOperator() override;
};

#endif //NORMAL_NORMAL_SQL_SRC_AST_S3SELECTSCANLOGICALOPERATOR_H
