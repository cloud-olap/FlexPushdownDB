//
// Created by matt on 15/4/20.
//

#include "normal/connector/s3/S3SelectPartitioningScheme.h"

std::vector<std::shared_ptr<S3SelectPartition>> S3SelectPartitioningScheme::partitions() {
  return partitions_;
}
