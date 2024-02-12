//
// Created by matt on 15/4/20.
//

#ifndef FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_OBJSTOREPARTITION_H
#define FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_OBJSTOREPARTITION_H

#include <fpdb/catalogue/Partition.h>
#include <fpdb/catalogue/Table.h>
#include <fpdb/caf/CAFUtil.h>
#include <string>
#include <memory>

using namespace std;

namespace fpdb::catalogue::obj_store {

class ObjStorePartition: public Partition {
public:
  explicit ObjStorePartition(string s3Bucket,
                             string s3Object);

  explicit ObjStorePartition(string s3Bucket,
                             string s3Object,
                             long numBytes);

  ObjStorePartition() = default;
  ObjStorePartition(const ObjStorePartition&) = default;
  ObjStorePartition& operator=(const ObjStorePartition&) = default;

  const string &getBucket() const;
  const string &getObject() const;
  const std::optional<int> &getNodeId() const;

  string toString() override;
  size_t hash() override;
  bool equalTo(shared_ptr<Partition> other) override;
  CatalogueEntryType getCatalogueEntryType() override;

  bool operator==(const ObjStorePartition& other);

  void setNodeId(int nodeId);

private:
  string s3Bucket_;
  string s3Object_;
  std::optional<int> nodeId_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ObjStorePartition& partition) {
    return f.object(partition).fields(f.field("numBytes", partition.numBytes_),
                                      f.field("zoneMap", partition.zoneMap_),
                                      f.field("bucket", partition.s3Bucket_),
                                      f.field("object", partition.s3Object_));
  }
};

}

using ObjStorePartitionPtr = std::shared_ptr<fpdb::catalogue::obj_store::ObjStorePartition>;

namespace caf {
template <>
struct inspector_access<ObjStorePartitionPtr> : variant_inspector_access<ObjStorePartitionPtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_OBJSTOREPARTITION_H
