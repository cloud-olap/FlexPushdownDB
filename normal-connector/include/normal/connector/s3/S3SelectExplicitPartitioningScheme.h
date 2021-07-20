//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTEXPLICITPARTITIONINGSCHEME_H
#define NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTEXPLICITPARTITIONINGSCHEME_H

#include <memory>
#include <utility>
#include <vector>

#include "S3SelectPartition.h"
#include "S3SelectPartitioningScheme.h"

class S3SelectExplicitPartitioningScheme  : public S3SelectPartitioningScheme {

public:
  S3SelectExplicitPartitioningScheme();

  void add(const std::shared_ptr<S3SelectPartition> &partition);

  std::shared_ptr<std::vector<std::shared_ptr<Partition>>> partitions() override;

private:
  std::shared_ptr<std::vector<std::shared_ptr<Partition>>> partitions_;

};

#endif //NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTEXPLICITPARTITIONINGSCHEME_H
