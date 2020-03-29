//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_CONNECTOR_CATALOGUEENTRY_H
#define NORMAL_NORMAL_SQL_INCLUDE_CONNECTOR_CATALOGUEENTRY_H

#include <string>
class CatalogueEntry {

private:
  std::string alias_;

public:
  explicit CatalogueEntry(std::string Alias);
  virtual ~CatalogueEntry() = default;

  [[nodiscard]] const std::string &getAlias() const;

};

#endif //NORMAL_NORMAL_SQL_INCLUDE_CONNECTOR_CATALOGUEENTRY_H
