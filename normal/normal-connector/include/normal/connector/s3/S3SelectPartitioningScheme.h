//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITIONINGSCHEME_H
#define NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITIONINGSCHEME_H

#include <vector>
#include <memory>
#include <normal/connector/partition/PartitioningScheme.h>
#include "S3SelectPartition.h"

class S3SelectPartitioningScheme  : public PartitioningScheme<S3SelectPartition> {

public:
  std::vector<std::shared_ptr<S3SelectPartition>> partitions() override;

protected:
  std::vector<std::shared_ptr<S3SelectPartition>> partitions_;

};

#endif //NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITIONINGSCHEME_H
