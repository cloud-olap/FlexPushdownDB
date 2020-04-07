//
// Created by matt on 27/3/20.
//

#include "connector/s3/S3SelectCatalogueEntry.h"
#include "logical/S3SelectScanLogicalOperator.h"

#include <utility>
#include <connector/s3/S3SelectConnector.h>
S3SelectCatalogueEntry::S3SelectCatalogueEntry(const std::string& Alias,
                                               std::string S3Bucket,
                                               std::string S3Object,
                                               std::shared_ptr<Catalogue> catalogue)
    : CatalogueEntry(Alias, std::move(catalogue)), s3Bucket_(std::move(S3Bucket)), s3Object_(std::move(S3Object)) {}

const std::string &S3SelectCatalogueEntry::s3Bucket() const {
  return s3Bucket_;
}

const std::string &S3SelectCatalogueEntry::s3Object() const {
  return s3Object_;
}

std::shared_ptr<ScanLogicalOperator> S3SelectCatalogueEntry::toLogicalOperator() {
  auto connector = std::static_pointer_cast<S3SelectConnector>(this->getCatalogue()->getConnector());

  return std::make_shared<S3SelectScanLogicalOperator>(this->s3Object_, this->s3Bucket_, connector->getAwsClient());
}
