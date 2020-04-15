//
// Created by matt on 15/4/20.
//

#include <normal/connector/local-fs/LocalFileExplicitPartitioningScheme.h>

void LocalFileExplicitPartitioningScheme::add(const std::shared_ptr<LocalFilePartition>& partition) {
  partitions_.push_back(partition);
}
