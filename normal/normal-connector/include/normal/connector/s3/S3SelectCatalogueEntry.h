//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_S3_S3SELECTCATALOGUEENTRY_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_S3_S3SELECTCATALOGUEENTRY_H

#include <string>

#include <normal/connector/CatalogueEntry.h>
#include <normal/connector/Catalogue.h>
#include <normal/sql/logical/ScanLogicalOperator.h>


namespace normal::connector::s3 {

class S3SelectCatalogueEntry: public normal::connector::CatalogueEntry {
private:
  std::string s3Bucket_;
  std::string s3Object_;
public:
  [[nodiscard]] const std::string &s3Bucket() const;
  [[nodiscard]] const std::string &s3Object() const;
public:
  S3SelectCatalogueEntry(const std::string& Alias, std::string S3Bucket, std::string S3Object, std::shared_ptr<Catalogue>);
  ~S3SelectCatalogueEntry() override = default;

  std::shared_ptr<sql::logical::ScanLogicalOperator> toLogicalOperator() override ;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_S3_S3SELECTCATALOGUEENTRY_H
