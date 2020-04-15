//
// Created by matt on 27/3/20.
//

#include <normal/connector/local-fs/LocalFileSystemCatalogueEntry.h>
#include "normal/plan/FileScanLogicalOperator.h"

using namespace normal::connector::local_fs;

LocalFileSystemCatalogueEntry::LocalFileSystemCatalogueEntry(
	const std::string &Alias,
	std::shared_ptr<LocalFilePartitioningScheme> partitioningScheme,
	std::shared_ptr<normal::connector::Catalogue> catalogue) :
	normal::connector::CatalogueEntry(Alias, std::move(catalogue)),
	partitioningScheme_(std::move(partitioningScheme)) {}

std::shared_ptr<normal::plan::ScanLogicalOperator> LocalFileSystemCatalogueEntry::toLogicalOperator() {
  auto op = std::make_shared<normal::plan::FileScanLogicalOperator>(partitioningScheme_);
  op->name = "fileScan";
  return op;
}
