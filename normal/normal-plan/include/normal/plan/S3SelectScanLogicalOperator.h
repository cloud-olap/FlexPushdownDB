//
// Created by matt on 7/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_S3SELECTSCANLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_S3SELECTSCANLOGICALOPERATOR_H

#include <memory>
#include <string>

#include <normal/core/Operator.h>
#include <normal/pushdown/AWSClient.h>

#include "ScanLogicalOperator.h"
#include <normal/connector/s3/S3SelectPartitioningScheme.h>

namespace normal::plan {

class S3SelectScanLogicalOperator: public ScanLogicalOperator {
private:
  std::shared_ptr<S3SelectPartitioningScheme> partitioningScheme_;
  std::shared_ptr<pushdown::AWSClient> awsClient_;

public:
  S3SelectScanLogicalOperator(std::shared_ptr<S3SelectPartitioningScheme> partitioningScheme,
                              std::shared_ptr<pushdown::AWSClient> AwsClient);

  std::shared_ptr<core::Operator> toOperator() override;
};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_S3SELECTSCANLOGICALOPERATOR_H
