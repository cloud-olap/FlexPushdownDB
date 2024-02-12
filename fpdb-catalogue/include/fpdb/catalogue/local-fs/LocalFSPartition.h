//
// Created by matt on 15/4/20.
//

#ifndef FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_LOCAL_FS_LOCALFSPARTITION_H
#define FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_LOCAL_FS_LOCALFSPARTITION_H

#include <fpdb/catalogue/Partition.h>
#include <fpdb/catalogue/Table.h>
#include <fpdb/caf/CAFUtil.h>
#include <string>
#include <memory>

namespace fpdb::catalogue::local_fs {

class LocalFSPartition: public Partition {
public:
  LocalFSPartition(const string &path);
  LocalFSPartition() = default;
  LocalFSPartition(const LocalFSPartition&) = default;
  LocalFSPartition& operator=(const LocalFSPartition&) = default;

  [[nodiscard]] const string &getPath() const;

  string toString() override;
  size_t hash() override;
  bool equalTo(shared_ptr<Partition> other) override;
  CatalogueEntryType getCatalogueEntryType() override;

  bool operator==(const LocalFSPartition& other);

private:
  string path_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LocalFSPartition& partition) {
    return f.object(partition).fields(f.field("numBytes", partition.numBytes_),
                                      f.field("zoneMap", partition.zoneMap_),
                                      f.field("path", partition.path_));
  }
};

}

using LocalFSPartitionPtr = std::shared_ptr<fpdb::catalogue::local_fs::LocalFSPartition>;

namespace caf {
template <>
struct inspector_access<LocalFSPartitionPtr> : variant_inspector_access<LocalFSPartitionPtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_LOCAL_FS_LOCALFSPARTITION_H
