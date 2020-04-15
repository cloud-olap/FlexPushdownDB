//
// Created by matt on 27/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_CATALOGUE_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_CATALOGUE_H

#include <string>
#include <unordered_map>
#include <memory>

#include "normal/sql/connector/Connector.h"
#include "normal/sql/connector/CatalogueEntry.h"

class CatalogueEntry;

class Catalogue {

private:
  std::string name_;
  std::unordered_map<std::string, std::shared_ptr<CatalogueEntry>> entries_;
  std::shared_ptr<Connector> connector_;
public:
  const std::shared_ptr<Connector> &getConnector() const;
public:
  explicit Catalogue(std::string Name, std::shared_ptr<Connector> Connector);
  virtual ~Catalogue() = default;

  [[nodiscard]] const std::string &getName() const;
  void put(const std::shared_ptr<CatalogueEntry>& entry);
  std::shared_ptr<CatalogueEntry> getEntry(const std::string& alias);
  std::string toString();
};

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_CONNECTOR_CATALOGUE_H
