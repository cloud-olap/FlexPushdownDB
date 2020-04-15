//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_EXPLICITPARTITIONINGSCHEME_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_EXPLICITPARTITIONINGSCHEME_H

#include <memory>
#include <utility>
#include <vector>

#include "PartitioningScheme.h"
#include "Partition.h"

class ExplicitPartitioningScheme : public PartitioningScheme {

public:
  explicit ExplicitPartitioningScheme(std::shared_ptr<std::vector<Partition>> Partitions) :
	  partitions_(std::move(Partitions)) {}

private:
  std::shared_ptr<std::vector<Partition>> partitions_;

};

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_PARTITION_EXPLICITPARTITIONINGSCHEME_H
