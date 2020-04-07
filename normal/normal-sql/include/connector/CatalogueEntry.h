//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_CONNECTOR_CATALOGUEENTRY_H
#define NORMAL_NORMAL_SQL_INCLUDE_CONNECTOR_CATALOGUEENTRY_H

#include <string>
#include "logical/ScanNode.h"
#include "connector/Catalogue.h"

class Catalogue;

class CatalogueEntry {

private:
  std::string alias_;
  std::shared_ptr<Catalogue> catalogue_;
public:
  const std::shared_ptr<Catalogue> &getCatalogue() const;
public:
  explicit CatalogueEntry(std::string Alias,
                          std::shared_ptr<Catalogue> Catalogue);
  virtual ~CatalogueEntry() = default;

  [[nodiscard]] const std::string &getAlias() const;

  virtual std::shared_ptr<ScanNode> toLogicalOperator() = 0;

};

#endif //NORMAL_NORMAL_SQL_INCLUDE_CONNECTOR_CATALOGUEENTRY_H
