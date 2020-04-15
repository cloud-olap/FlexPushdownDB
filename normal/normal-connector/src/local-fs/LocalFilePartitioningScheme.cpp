//
// Created by matt on 15/4/20.
//

#include <normal/connector/local-fs/LocalFilePartitioningScheme.h>

std::vector<std::shared_ptr<LocalFilePartition>> LocalFilePartitioningScheme::partitions() {
  return partitions_;
}
