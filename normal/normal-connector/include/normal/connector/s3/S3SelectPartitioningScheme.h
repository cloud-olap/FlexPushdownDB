//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITIONINGSCHEME_H
#define NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITIONINGSCHEME_H

#include <vector>
#include <memory>
#include <normal/connector/partition/PartitioningScheme.h>
#include "S3SelectPartition.h"

/**
 * Abstract class representing a collection of s3 partitions
 */
class S3SelectPartitioningScheme  : public PartitioningScheme {
};

#endif //NORMAL_NORMAL_CONNECTOR_INCLUDE_NORMAL_CONNECTOR_S3_S3SELECTPARTITIONINGSCHEME_H
