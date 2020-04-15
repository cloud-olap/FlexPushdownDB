//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_S3_S3SELECTCATALOGUEENTRY_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_S3_S3SELECTCATALOGUEENTRY_H

#include <string>

#include <normal/connector/CatalogueEntry.h>
#include <normal/connector/Catalogue.h>
#include "normal/plan/ScanLogicalOperator.h"
#include "S3SelectPartitioningScheme.h"

namespace normal::connector::s3 {

class S3SelectCatalogueEntry : public normal::connector::CatalogueEntry {

public:
  S3SelectCatalogueEntry(const std::string &Alias,
						 std::shared_ptr<S3SelectPartitioningScheme> partitioningScheme,
						 std::shared_ptr<Catalogue>);
  ~S3SelectCatalogueEntry() override = default;

  std::shared_ptr<plan::ScanLogicalOperator> toLogicalOperator() override;

private:
  std::shared_ptr<S3SelectPartitioningScheme> partitioningScheme_;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_S3_S3SELECTCATALOGUEENTRY_H
