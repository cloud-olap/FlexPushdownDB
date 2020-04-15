//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCATALOGUEENTRY_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCATALOGUEENTRY_H

#include <memory>
#include <string>

#include <normal/connector/CatalogueEntry.h>
#include <normal/sql/logical/ScanLogicalOperator.h>
#include <normal/connector/Catalogue.h>
#include <normal/connector/local-fs/LocalFilePartitioningScheme.h>

namespace normal::connector::local_fs {

class LocalFileSystemCatalogueEntry : public normal::connector::CatalogueEntry {

private:
  std::shared_ptr<LocalFilePartitioningScheme> partitioningScheme_;

public:
  LocalFileSystemCatalogueEntry(const std::string &Alias,
								std::shared_ptr<LocalFilePartitioningScheme> partitioningScheme,
								std::shared_ptr<Catalogue>);
  ~LocalFileSystemCatalogueEntry() override = default;

  std::shared_ptr<sql::logical::ScanLogicalOperator> toLogicalOperator() override;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCATALOGUEENTRY_H
