//
// Created by Yifei Yang on 2/27/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAF_SERIALIZATION_CAFFILESCANKERNELSERIALIZER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAF_SERIALIZATION_CAFFILESCANKERNELSERIALIZER_H

#include <fpdb/executor/physical/file/LocalFileScanKernel.h>
#include <fpdb/executor/physical/file/RemoteFileScanKernel.h>
#include <fpdb/caf/CAFUtil.h>

using namespace fpdb::executor::physical::file;
using FileScanKernelPtr = std::shared_ptr<FileScanKernel>;

CAF_BEGIN_TYPE_ID_BLOCK(FileScanKernel, fpdb::caf::CAFUtil::FileScanKernel_first_custom_type_id)
CAF_ADD_TYPE_ID(FileScanKernel, (FileScanKernelPtr))
CAF_ADD_TYPE_ID(FileScanKernel, (LocalFileScanKernel))
CAF_ADD_TYPE_ID(FileScanKernel, (RemoteFileScanKernel))
CAF_END_TYPE_ID_BLOCK(FileScanKernel)

// Variant-based approach on FileScanKernelPtr
namespace caf {

template<>
struct variant_inspector_traits<FileScanKernelPtr> {
  using value_type = FileScanKernelPtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<LocalFileScanKernel>,
          type_id_v<RemoteFileScanKernel>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->getType() == CatalogueEntryType::LOCAL_FS)
      return 1;
    else
      return 2;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(dynamic_cast<LocalFileScanKernel &>(*x));
      case 2:
        return f(dynamic_cast<RemoteFileScanKernel &>(*x));
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
      case type_id_v<LocalFileScanKernel>: {
        auto tmp = LocalFileScanKernel{};
        continuation(tmp);
        return true;
      }
      case type_id_v<RemoteFileScanKernel>: {
        auto tmp = RemoteFileScanKernel{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<FileScanKernelPtr> : variant_inspector_access<FileScanKernelPtr> {
  // nop
};

} // namespace caf

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAF_SERIALIZATION_CAFFILESCANKERNELSERIALIZER_H
