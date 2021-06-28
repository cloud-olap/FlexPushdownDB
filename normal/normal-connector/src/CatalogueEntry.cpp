//
// Created by matt on 27/3/20.
//

#include <normal/connector/CatalogueEntry.h>

normal::connector::CatalogueEntry::CatalogueEntry(std::string Alias, std::shared_ptr<Catalogue> Catalogue) :
  alias_(std::move(Alias)), catalogue_(std::move(Catalogue)) {}

const std::string &normal::connector::CatalogueEntry::getAlias() const {
  return alias_;
}

const std::shared_ptr<normal::connector::Catalogue> &normal::connector::CatalogueEntry::getCatalogue() const {
  return catalogue_;
}
