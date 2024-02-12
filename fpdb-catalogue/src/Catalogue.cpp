//
// Created by matt on 27/3/20.
//

#include <fpdb/catalogue/Catalogue.h>

namespace fpdb::catalogue {

Catalogue::Catalogue(string Name, filesystem::path metadataPath) :
  name_(move(Name)),
  metadataPath_(move(metadataPath)) {}

const string &Catalogue::getName() const {
  return name_;
}

void Catalogue::putEntry(const shared_ptr<CatalogueEntry>& entry) {
  this->entries_.emplace(entry->getName(), entry);
}

tl::expected<shared_ptr<CatalogueEntry>, string> Catalogue::getEntry(const string& name) {
  auto entryIterator = this->entries_.find(name);
  if(entryIterator == this->entries_.end()){
    return tl::unexpected("Catalogue entry '" + name + "' not found");
  }
  else{
    return entryIterator->second;
  }
}

filesystem::path Catalogue::getMetadataPath() const {
  return metadataPath_;
}

}
