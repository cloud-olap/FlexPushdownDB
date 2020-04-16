//
// Created by matt on 15/4/20.
//

#include "normal/connector/s3/S3SelectExplicitPartitioningScheme.h"

S3SelectExplicitPartitioningScheme::S3SelectExplicitPartitioningScheme() :
	partitions_(std::make_shared<std::vector<std::shared_ptr<Partition>>>()) {}

void S3SelectExplicitPartitioningScheme::add(const std::shared_ptr<S3SelectPartition> &partition) {
  partitions_->push_back(partition);
}

std::shared_ptr<std::vector<std::shared_ptr<Partition>>> S3SelectExplicitPartitioningScheme::partitions() {
  return partitions_;
}
