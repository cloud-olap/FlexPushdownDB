//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_PARTITIONINGSCHEME_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_PARTITIONINGSCHEME_H

#include <vector>
#include <memory>
#include "Partition.h"

/**
 * Base class for a partitioning scheme, i.e. how to break up and parallelise a data source
 */
class PartitioningScheme {

public:
  virtual ~PartitioningScheme() = default;

  /**
   * Returns the partitions
   *
   * @return
   */
  virtual std::shared_ptr<std::vector<std::shared_ptr<Partition>>> partitions() = 0;

};

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_PARTITIONINGSCHEME_H
