//
// Created by matt on 15/4/20.
//

#include <normal/connector/local-fs/LocalFileExplicitPartitioningScheme.h>

LocalFileExplicitPartitioningScheme::LocalFileExplicitPartitioningScheme() :
	partitions_(std::make_shared<std::vector<std::shared_ptr<Partition>>>()) {}

void LocalFileExplicitPartitioningScheme::add(const std::shared_ptr<LocalFilePartition> &partition) {
  partitions_->push_back(partition);
}

std::shared_ptr<std::vector<std::shared_ptr<Partition>>> LocalFileExplicitPartitioningScheme::partitions() {
  return partitions_;
}
