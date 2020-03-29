//
// Created by matt on 27/3/20.
//

#include "connector/local-fs/LocalFileSystemCatalogueEntry.h"

#include <utility>
LocalFileSystemCatalogueEntry::LocalFileSystemCatalogueEntry(const std::string &Alias, std::string Path)
    : CatalogueEntry(Alias), path_(std::move(Path)) {}
const std::string &LocalFileSystemCatalogueEntry::getPath() const {
  return path_;
}
