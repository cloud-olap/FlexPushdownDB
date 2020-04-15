//
// Created by matt on 27/3/20.
//

#include <normal/sql/connector/local-fs/LocalFileSystemCatalogueEntry.h>
#include <normal/sql/logical/FileScanLogicalOperator.h>

using namespace normal::sql::connector::local_fs;

LocalFileSystemCatalogueEntry::LocalFileSystemCatalogueEntry(const std::string &Alias, std::string Path,
															 std::shared_ptr<normal::sql::connector::Catalogue> catalogue)
	: normal::sql::connector::CatalogueEntry(Alias, std::move(catalogue)), path_(std::move(Path)) {}

const std::string &LocalFileSystemCatalogueEntry::getPath() const {
  return path_;
}

std::shared_ptr<normal::sql::logical::ScanLogicalOperator> LocalFileSystemCatalogueEntry::toLogicalOperator() {
  auto op = std::make_shared<normal::sql::logical::FileScanLogicalOperator>(this->path_);
  op->name = "fileScan";
  return op;
}
