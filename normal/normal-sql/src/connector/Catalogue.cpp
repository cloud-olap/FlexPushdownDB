//
// Created by matt on 27/3/20.
//

#include "connector/Catalogue.h"

#include <utility>
#include <sstream>

Catalogue::Catalogue(std::string Name, std::shared_ptr<Connector> Connector)
    : name_(std::move(Name)), connector_(Connector) {}

const std::string &Catalogue::getName() const {
  return name_;
}

void Catalogue::put(std::shared_ptr<CatalogueEntry> entry) {
  this->entries_.emplace(entry->getAlias(), entry);
}

std::shared_ptr<CatalogueEntry> Catalogue::getEntry(std::string alias) {
  return this->entries_.find(alias)->second;
}

std::string Catalogue::toString() {
  std::stringstream ss;
  for(const auto& entry : entries_){
    ss << entry.second->getAlias() << std::endl;
  }
  return ss.str();
}

const std::shared_ptr<Connector> &Catalogue::getConnector() const {
  return connector_;
}
