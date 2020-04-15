//
// Created by matt on 27/3/20.
//

#include <normal/sql/connector/s3/S3SelectCatalogueEntry.h>
#include <normal/sql/logical/S3SelectScanLogicalOperator.h>

#include <normal/sql/connector/s3/S3SelectConnector.h>

normal::sql::connector::s3::S3SelectCatalogueEntry::S3SelectCatalogueEntry(const std::string& Alias,
                                                                           std::string S3Bucket,
                                                                           std::string S3Object,
                                                                           std::shared_ptr<normal::sql::connector::Catalogue> catalogue)
    : normal::sql::connector::CatalogueEntry(Alias, std::move(catalogue)), s3Bucket_(std::move(S3Bucket)), s3Object_(std::move(S3Object)) {}

const std::string &normal::sql::connector::s3::S3SelectCatalogueEntry::s3Bucket() const {
  return s3Bucket_;
}

const std::string &normal::sql::connector::s3::S3SelectCatalogueEntry::s3Object() const {
  return s3Object_;
}

std::shared_ptr<normal::sql::logical::ScanLogicalOperator> normal::sql::connector::s3::S3SelectCatalogueEntry::toLogicalOperator() {
  auto connector = std::static_pointer_cast<S3SelectConnector>(this->getCatalogue()->getConnector());

  return std::make_shared<normal::sql::logical::S3SelectScanLogicalOperator>(this->s3Object_, this->s3Bucket_, connector->getAwsClient());
}
