//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCATALOGUEENTRY_H
#define NORMAL_NORMAL_SQL_SRC_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCATALOGUEENTRY_H

#include <string>
#include <connector/CatalogueEntry.h>
#include <memory>
#include "logical/ScanNode.h"

class LocalFileSystemCatalogueEntry: public CatalogueEntry {

private:
  std::string path_;
public:
  const std::string &getPath() const;
public:
  LocalFileSystemCatalogueEntry(const std::string &Alias, std::string Path, std::shared_ptr<Catalogue>);
  ~LocalFileSystemCatalogueEntry() override = default;

  std::shared_ptr<ScanNode> toLogicalOperator() override ;

};

#endif //NORMAL_NORMAL_SQL_SRC_CONNECTOR_LOCAL_FS_LOCALFILESYSTEMCATALOGUEENTRY_H
