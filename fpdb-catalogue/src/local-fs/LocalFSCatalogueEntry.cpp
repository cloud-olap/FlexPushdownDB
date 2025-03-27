//
// Created by matt on 27/3/20.
//

#include <fpdb/catalogue/local-fs/LocalFSCatalogueEntry.h>
#include <fpdb/catalogue/CatalogueEntryType.h>
#include <fmt/format.h>
#include <utility>

namespace fpdb::catalogue::local_fs {

LocalFSCatalogueEntry::LocalFSCatalogueEntry(string schemaName,
                                             shared_ptr<Catalogue> catalogue) :
  CatalogueEntry(LOCAL_FS,
                 move(schemaName),
                 move(catalogue)) {}

string LocalFSCatalogueEntry::getTypeName() const {
  return "LocalFSCatalogueEntry";
}

string LocalFSCatalogueEntry::getName() const {
  return fmt::format("local-fs://{}", getSchemaName());
}

bool LocalFSCatalogueEntry::isFKey(const string &, const vector<string> &,
                                   const string &, const vector<string> &) const {
  throw std::runtime_error("Checking foreign-key in local-fs is unsupported.");
}

}
