//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCATALOGUEENTRY_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCATALOGUEENTRY_H

#include <memory>
#include <string>

#include <normal/connector/CatalogueEntry.h>
#include "normal/plan/operator_/ScanLogicalOperator.h"
#include <normal/connector/Catalogue.h>
#include <normal/connector/local-fs/LocalFilePartitioningScheme.h>

namespace normal::connector::local_fs {

class LocalFileSystemCatalogueEntry : public normal::connector::CatalogueEntry {

public:
  LocalFileSystemCatalogueEntry(const std::string &Alias,
								std::shared_ptr<LocalFilePartitioningScheme> partitioningScheme,
								std::shared_ptr<Catalogue>);
  ~LocalFileSystemCatalogueEntry() override = default;

  std::shared_ptr<normal::plan::operator_::ScanLogicalOperator> toLogicalOperator() override;

private:
  std::shared_ptr<LocalFilePartitioningScheme> partitioningScheme_;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCATALOGUEENTRY_H
