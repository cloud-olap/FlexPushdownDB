//
// Created by matt on 27/3/20.
//

#include <normal/connector/s3/S3SelectCatalogueEntry.h>
#include "normal/plan/operator_/S3SelectScanLogicalOperator.h"

#include <normal/connector/s3/S3SelectConnector.h>

normal::connector::s3::S3SelectCatalogueEntry::S3SelectCatalogueEntry(const std::string& Alias,
																	  std::shared_ptr<S3SelectPartitioningScheme> partitioningScheme,
                                                                           std::shared_ptr<normal::connector::Catalogue> catalogue)
    : normal::connector::CatalogueEntry(Alias, std::move(catalogue)), partitioningScheme_(std::move(partitioningScheme)) {}

std::shared_ptr<normal::plan::operator_::ScanLogicalOperator> normal::connector::s3::S3SelectCatalogueEntry::toLogicalOperator() {
  auto connector = std::static_pointer_cast<S3SelectConnector>(this->getCatalogue()->getConnector());
  auto op = std::make_shared<normal::plan::operator_::S3SelectScanLogicalOperator>(partitioningScheme_, connector->getAwsClient());
  return op;
}
