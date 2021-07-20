//
// Created by matt on 27/3/20.
//

#include <normal/connector/Catalogue.h>

using namespace normal::connector;

Catalogue::Catalogue(std::string Name, std::shared_ptr<Connector> Connector)
    : name_(std::move(Name)), connector_(std::move(Connector)) {}

const std::string &Catalogue::getName() const {
  return name_;
}

void Catalogue::put(const std::shared_ptr<CatalogueEntry>& entry) {
  this->entries_.emplace(entry->getAlias(), entry);
}

tl::expected<std::shared_ptr<CatalogueEntry>, std::string> Catalogue::entry(const std::string& name) {
  auto entryIterator = this->entries_.find(name);
  if(entryIterator == this->entries_.end()){
    return tl::unexpected("Catalogue entry '" + name + "' not found");
  }
  else{
    return entryIterator->second;
  }
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
