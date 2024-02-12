//
// Created by Yifei Yang on 3/28/22.
//

#ifndef FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_CAF_SERIALIZATION_CAFOBJSTORECONNECTORSERIALIZER_H
#define FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_CAF_SERIALIZATION_CAFOBJSTORECONNECTORSERIALIZER_H

#include <fpdb/catalogue/obj-store/s3/S3Connector.h>
#include <fpdb/catalogue/obj-store/fpdb-store/FPDBStoreConnector.h>
#include <fpdb/caf/CAFUtil.h>

using ObjStoreConnectorPtr = std::shared_ptr<fpdb::catalogue::obj_store::ObjStoreConnector>;

CAF_BEGIN_TYPE_ID_BLOCK(ObjStoreConnector, fpdb::caf::CAFUtil::ObjStoreConnector_first_custom_type_id)
CAF_ADD_TYPE_ID(ObjStoreConnector, (ObjStoreConnectorPtr))
CAF_ADD_TYPE_ID(ObjStoreConnector, (fpdb::catalogue::obj_store::S3Connector))
CAF_ADD_TYPE_ID(ObjStoreConnector, (fpdb::catalogue::obj_store::FPDBStoreConnector))
CAF_END_TYPE_ID_BLOCK(ObjStoreConnector)

namespace caf {

template<>
struct variant_inspector_traits<ObjStoreConnectorPtr> {
  using value_type = ObjStoreConnectorPtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<fpdb::catalogue::obj_store::S3Connector>,
          type_id_v<fpdb::catalogue::obj_store::FPDBStoreConnector>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->getStoreType() == fpdb::catalogue::obj_store::ObjStoreType::S3)
      return 1;
    else if (x->getStoreType() == fpdb::catalogue::obj_store::ObjStoreType::FPDB_STORE)
      return 2;
    else
      return -1;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(dynamic_cast<fpdb::catalogue::obj_store::S3Connector &>(*x));
      case 2:
        return f(dynamic_cast<fpdb::catalogue::obj_store::FPDBStoreConnector &>(*x));
      default: {
        none_t dummy;
        return f(dummy);
      }
    }
  }

  // Assigns a value to x.
  template<class U>
  static void assign(value_type &x, U value) {
    if constexpr (std::is_same<U, none_t>::value)
      x.reset();
    else
      x = std::make_shared<U>(std::move(value));
  }

  // Create a default-constructed object for `type` and then call the
  // continuation with the temporary object to perform remaining load steps.
  template<class F>
  static bool load(type_id_t type, F continuation) {
    switch (type) {
      default:
        return false;
      case type_id_v<none_t>: {
        none_t dummy;
        continuation(dummy);
        return true;
      }
      case type_id_v<fpdb::catalogue::obj_store::S3Connector>: {
        auto tmp = fpdb::catalogue::obj_store::S3Connector{};
        continuation(tmp);
        return true;
      }
      case type_id_v<fpdb::catalogue::obj_store::FPDBStoreConnector>: {
        auto tmp = fpdb::catalogue::obj_store::FPDBStoreConnector{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<ObjStoreConnectorPtr> : variant_inspector_access<ObjStoreConnectorPtr> {
  // nop
};

} // namespace caf

#endif //FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_CAF_SERIALIZATION_CAFOBJSTORECONNECTORSERIALIZER_H
