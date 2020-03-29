//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_S3SELECTCATALOGUEENTRY_H
#define NORMAL_NORMAL_SQL_SRC_S3SELECTCATALOGUEENTRY_H

#include <string>
#include <connector/CatalogueEntry.h>

class S3SelectCatalogueEntry: public CatalogueEntry {
private:
  std::string s3Bucket_;
  std::string s3Object_;
public:
  S3SelectCatalogueEntry(const std::string& Alias, std::string S3Bucket, std::string S3Object);
  ~S3SelectCatalogueEntry() override = default;

};

#endif //NORMAL_NORMAL_SQL_SRC_S3SELECTCATALOGUEENTRY_H
