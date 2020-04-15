//
// Created by matt on 27/3/20.
//

#include <normal/sql/connector/Catalogue.h>

normal::sql::connector::Catalogue::Catalogue(std::string Name, std::shared_ptr<Connector> Connector)
    : name_(std::move(Name)), connector_(std::move(Connector)) {}

const std::string &normal::sql::connector::Catalogue::getName() const {
  return name_;
}

void normal::sql::connector::Catalogue::put(const std::shared_ptr<normal::sql::connector::CatalogueEntry>& entry) {
  this->entries_.emplace(entry->getAlias(), entry);
}

std::shared_ptr<normal::sql::connector::CatalogueEntry> normal::sql::connector::Catalogue::getEntry(const std::string& alias) {
  return this->entries_.find(alias)->second;
}

std::string normal::sql::connector::Catalogue::toString() {
  std::stringstream ss;
  for(const auto& entry : entries_){
    ss << entry.second->getAlias() << std::endl;
  }
  return ss.str();
}

const std::shared_ptr<normal::sql::connector::Connector> &normal::sql::connector::Catalogue::getConnector() const {
  return connector_;
}
