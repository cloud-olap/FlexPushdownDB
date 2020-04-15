//
// Created by matt on 27/3/20.
//

#include "normal/sql/connector/CatalogueEntry.h"

#include <utility>

CatalogueEntry::CatalogueEntry(std::string Alias,
                               std::shared_ptr<Catalogue> Catalogue) : alias_(std::move(Alias)), catalogue_(std::move(Catalogue)) {}

const std::string &CatalogueEntry::getAlias() const {
  return alias_;
}

const std::shared_ptr<Catalogue> &CatalogueEntry::getCatalogue() const {
  return catalogue_;
}
