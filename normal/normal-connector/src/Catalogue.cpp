//
// Created by matt on 27/3/20.
//

#include <normal/connector/Catalogue.h>

normal::connector::Catalogue::Catalogue(std::string Name, std::shared_ptr<Connector> Connector)
    : name_(std::move(Name)), connector_(std::move(Connector)) {}

const std::string &normal::connector::Catalogue::getName() const {
  return name_;
}

void normal::connector::Catalogue::put(const std::shared_ptr<normal::connector::CatalogueEntry>& entry) {
  this->entries_.emplace(entry->getAlias(), entry);
}

std::shared_ptr<normal::connector::CatalogueEntry> normal::connector::Catalogue::getEntry(const std::string& alias) {
  return this->entries_.find(alias)->second;
}

std::string normal::connector::Catalogue::toString() {
  std::stringstream ss;
  for(const auto& entry : entries_){
    ss << entry.second->getAlias() << std::endl;
  }
  return ss.str();
}

const std::shared_ptr<normal::connector::Connector> &normal::connector::Catalogue::getConnector() const {
  return connector_;
}
