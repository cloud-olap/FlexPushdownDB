//
// Created by matt on 27/3/20.
//

#include <normal/sql/connector/CatalogueEntry.h>

normal::sql::connector::CatalogueEntry::CatalogueEntry(std::string Alias,
                                                       std::shared_ptr<Catalogue> Catalogue) : alias_(std::move(Alias)), catalogue_(std::move(Catalogue)) {}

const std::string &normal::sql::connector::CatalogueEntry::getAlias() const {
  return alias_;
}

const std::shared_ptr<normal::sql::connector::Catalogue> &normal::sql::connector::CatalogueEntry::getCatalogue() const {
  return catalogue_;
}
