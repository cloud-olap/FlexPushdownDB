//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_EXPLICITPARTITIONINGSCHEME_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_EXPLICITPARTITIONINGSCHEME_H

#include <memory>
#include <utility>
#include <vector>

#include <normal/connector/local-fs/LocalFilePartitioningScheme.h>
#include <normal/connector/local-fs/LocalFilePartition.h>

class LocalFileExplicitPartitioningScheme : public LocalFilePartitioningScheme {

public:
  explicit LocalFileExplicitPartitioningScheme() = default;

  void add(const std::shared_ptr<LocalFilePartition> &partition);

};

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_EXPLICITPARTITIONINGSCHEME_H
