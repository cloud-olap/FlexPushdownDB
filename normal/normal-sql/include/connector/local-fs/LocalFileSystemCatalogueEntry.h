//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCATALOGUEENTRY_H
#define NORMAL_NORMAL_SQL_SRC_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCATALOGUEENTRY_H

#include <memory>
#include <string>

#include <connector/CatalogueEntry.h>
#include "logical/ScanLogicalOperator.h"

class LocalFileSystemCatalogueEntry: public CatalogueEntry {

private:
  std::string path_;

public:
  LocalFileSystemCatalogueEntry(const std::string &Alias, std::string Path, std::shared_ptr<Catalogue>);
  ~LocalFileSystemCatalogueEntry() override = default;

  [[nodiscard]] const std::string &getPath() const;

  std::shared_ptr<ScanLogicalOperator> toLogicalOperator() override ;

};

#endif //NORMAL_NORMAL_SQL_SRC_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCATALOGUEENTRY_H
