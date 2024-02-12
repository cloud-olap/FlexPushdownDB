//
// Created by Yifei Yang on 3/1/22.
//

#include <fpdb/catalogue/obj-store/ObjStoreConnector.h>

namespace fpdb::catalogue::obj_store {

ObjStoreConnector::ObjStoreConnector(ObjStoreType storeType):
  storeType_(storeType) {}

ObjStoreType ObjStoreConnector::getStoreType() const {
  return storeType_;
}
  
}
