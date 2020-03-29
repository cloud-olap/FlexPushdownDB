//
// Created by matt on 27/3/20.
//

#include "connector/CatalogueEntry.h"

#include <utility>
CatalogueEntry::CatalogueEntry(std::string Alias) : alias_(std::move(Alias)) {}
const std::string &CatalogueEntry::getAlias() const {
  return alias_;
}
