//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_LOCALFILEPARTITIONINGSCHEME_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_LOCALFILEPARTITIONINGSCHEME_H

#include <vector>
#include <memory>
#include <normal/connector/partition/PartitioningScheme.h>
#include "LocalFilePartition.h"

/**
 * Abstract class representing a collection of local file system partitions
 */
class LocalFilePartitioningScheme : public PartitioningScheme<LocalFilePartition> {

public:
  std::vector<std::shared_ptr<LocalFilePartition>> partitions() override;

protected:
  std::vector<std::shared_ptr<LocalFilePartition>> partitions_;

};

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_LOCALFILEPARTITIONINGSCHEME_H
