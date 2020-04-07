//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_S3SELECTCATALOGUEENTRY_H
#define NORMAL_NORMAL_SQL_SRC_S3SELECTCATALOGUEENTRY_H

#include <string>
#include <connector/CatalogueEntry.h>
#include "logical/ScanLogicalOperator.h"

class S3SelectCatalogueEntry: public CatalogueEntry {
private:
  std::string s3Bucket_;
  std::string s3Object_;
public:
  [[nodiscard]] const std::string &s3Bucket() const;
  [[nodiscard]] const std::string &s3Object() const;
public:
  S3SelectCatalogueEntry(const std::string& Alias, std::string S3Bucket, std::string S3Object, std::shared_ptr<Catalogue>);
  ~S3SelectCatalogueEntry() override = default;

  std::shared_ptr<ScanLogicalOperator> toLogicalOperator() override ;

};

#endif //NORMAL_NORMAL_SQL_SRC_S3SELECTCATALOGUEENTRY_H
