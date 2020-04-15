//
// Created by matt on 15/4/20.
//

#include "normal/connector/s3/S3SelectExplicitPartitioningScheme.h"

void S3SelectExplicitPartitioningScheme::add(const std::shared_ptr<S3SelectPartition> &partition) {
  partitions_.push_back(partition);
}
