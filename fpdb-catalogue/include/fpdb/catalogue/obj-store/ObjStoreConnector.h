//
// Created by Yifei Yang on 3/1/22.
//

#ifndef FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_OBJSTORECONNECTOR_H
#define FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_OBJSTORECONNECTOR_H

#include <fpdb/catalogue/obj-store/ObjStoreType.h>

namespace fpdb::catalogue::obj_store {

class ObjStoreConnector {

public:
  ObjStoreConnector(ObjStoreType type);
  ObjStoreConnector() = default;
  ObjStoreConnector(const ObjStoreConnector&) = default;
  ObjStoreConnector& operator=(const ObjStoreConnector&) = default;
  virtual ~ObjStoreConnector() = default;

  ObjStoreType getStoreType() const;

protected:
  ObjStoreType storeType_;
  
};

}


#endif //FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_OBJ_STORE_OBJSTORECONNECTOR_H
