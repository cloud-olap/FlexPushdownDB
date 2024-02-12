//
// Created by Yifei Yang on 1/16/22.
//

#ifndef FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_CAFSERIALIZATION_CAFTABLESERIALIZER_H
#define FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_CAFSERIALIZATION_CAFTABLESERIALIZER_H

#include <fpdb/catalogue/obj-store/ObjStoreTable.h>
#include <fpdb/catalogue/local-fs/LocalFSTable.h>
#include <fpdb/caf/CAFUtil.h>

using TablePtr = std::shared_ptr<fpdb::catalogue::Table>;

CAF_BEGIN_TYPE_ID_BLOCK(Table, fpdb::caf::CAFUtil::Table_first_custom_type_id)
CAF_ADD_TYPE_ID(Table, (TablePtr))
CAF_ADD_TYPE_ID(Table, (fpdb::catalogue::obj_store::ObjStoreTable))
CAF_ADD_TYPE_ID(Table, (fpdb::catalogue::local_fs::LocalFSTable))
CAF_END_TYPE_ID_BLOCK(Table)

namespace caf {

template<>
struct variant_inspector_traits<TablePtr> {
  using value_type = TablePtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<fpdb::catalogue::obj_store::ObjStoreTable>,
          type_id_v<fpdb::catalogue::local_fs::LocalFSTable>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->getCatalogueEntryType() == fpdb::catalogue::OBJ_STORE)
      return 1;
    else if (x->getCatalogueEntryType() == fpdb::catalogue::LOCAL_FS)
      return 2;
    else
      return -1;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(dynamic_cast<fpdb::catalogue::obj_store::ObjStoreTable &>(*x));
      case 2:
        return f(dynamic_cast<fpdb::catalogue::local_fs::LocalFSTable &>(*x));
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
      case type_id_v<fpdb::catalogue::obj_store::ObjStoreTable>: {
        auto tmp = fpdb::catalogue::obj_store::ObjStoreTable{};
        continuation(tmp);
        return true;
      }
      case type_id_v<fpdb::catalogue::local_fs::LocalFSTable>: {
        auto tmp = fpdb::catalogue::local_fs::LocalFSTable{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<TablePtr> : variant_inspector_access<TablePtr> {
  // nop
};

} // namespace caf

#endif //FPDB_FPDB_CATALOGUE_INCLUDE_FPDB_CATALOGUE_CAFSERIALIZATION_CAFTABLESERIALIZER_H
