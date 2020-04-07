//
// Created by matt on 27/3/20.
//

#include "connector/local-fs/LocalFileSystemCatalogueEntry.h"
#include "../../ast/FileScanLogicalOperator.h"

#include <utility>
LocalFileSystemCatalogueEntry::LocalFileSystemCatalogueEntry(const std::string &Alias, std::string Path,
                                                             std::shared_ptr<Catalogue> catalogue)
    : CatalogueEntry(Alias, catalogue), path_(std::move(Path)) {}

const std::string &LocalFileSystemCatalogueEntry::getPath() const {
  return path_;
}

std::shared_ptr<ScanNode> LocalFileSystemCatalogueEntry::toLogicalOperator() {
  return std::make_shared<FileScanLogicalOperator>(this->path_);
}
