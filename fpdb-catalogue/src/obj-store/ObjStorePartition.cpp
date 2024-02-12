//
// Created by matt on 15/4/20.
//

#include <fpdb/catalogue/obj-store//ObjStorePartition.h>
#include <utility>

namespace fpdb::catalogue::obj_store {

ObjStorePartition::ObjStorePartition(string s3Bucket,
                                     string s3Object) :
	s3Bucket_(move(s3Bucket)),
	s3Object_(move(s3Object)) {}

ObjStorePartition::ObjStorePartition(string s3Bucket,
                                     string s3Object,
                                     long numBytes) :
  s3Bucket_(move(s3Bucket)),
  s3Object_(move(s3Object)) {
  setNumBytes(numBytes);
}

const string &ObjStorePartition::getBucket() const {
  return s3Bucket_;
}

const string &ObjStorePartition::getObject() const {
  return s3Object_;
}

const std::optional<int> &ObjStorePartition::getNodeId() const {
  return nodeId_;
}

string ObjStorePartition::toString() {
  return "s3://" + s3Bucket_ + "/" + s3Object_;
}

size_t ObjStorePartition::hash() {
  return std::hash<string>()("s3://" + s3Bucket_ + "/" + s3Object_);
}

bool ObjStorePartition::equalTo(shared_ptr<Partition> other) {
  auto typedOther = static_pointer_cast<const ObjStorePartition>(other);
  if (!typedOther) {
	  return false;
  } else{
	  return this->operator==(*typedOther);
  }
}

CatalogueEntryType ObjStorePartition::getCatalogueEntryType() {
  return OBJ_STORE;
}

bool ObjStorePartition::operator==(const ObjStorePartition &other) {
  return s3Bucket_ == other.s3Bucket_ && s3Object_ == other.s3Object_;
}

void ObjStorePartition::setNodeId(int nodeId) {
  nodeId_ = nodeId;
}

}
