//
// Created by Yifei Yang on 4/20/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAF_SERIALIZATION_CAFGROUPABSTRACTKERNELSERIALIZER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAF_SERIALIZATION_CAFGROUPABSTRACTKERNELSERIALIZER_H

#include <fpdb/executor/physical/group/GroupKernel.h>
#include <fpdb/executor/physical/group/GroupArrowKernel.h>
#include <fpdb/caf/CAFUtil.h>

using namespace fpdb::executor::physical::group;
using GroupAbstractKernelPtr = std::shared_ptr<GroupAbstractKernel>;

CAF_BEGIN_TYPE_ID_BLOCK(GroupAbstractKernel, fpdb::caf::CAFUtil::GroupAbstractKernel_first_custom_type_id)
CAF_ADD_TYPE_ID(GroupAbstractKernel, (GroupAbstractKernelPtr))
CAF_ADD_TYPE_ID(GroupAbstractKernel, (GroupKernel))
CAF_ADD_TYPE_ID(GroupAbstractKernel, (GroupArrowKernel))
CAF_END_TYPE_ID_BLOCK(GroupAbstractKernel)

// Variant-based approach on GroupAbstractKernelPtr
namespace caf {

template<>
struct variant_inspector_traits<GroupAbstractKernelPtr> {
  using value_type = GroupAbstractKernelPtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<GroupKernel>,
          type_id_v<GroupArrowKernel>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->getType() == GroupKernelType::GROUP_KERNEL)
      return 1;
    else
      return 2;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(dynamic_cast<GroupKernel &>(*x));
      case 2:
        return f(dynamic_cast<GroupArrowKernel &>(*x));
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
      case type_id_v<GroupKernel>: {
        auto tmp = GroupKernel{};
        continuation(tmp);
        return true;
      }
      case type_id_v<GroupArrowKernel>: {
        auto tmp = GroupArrowKernel{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<GroupAbstractKernelPtr> : variant_inspector_access<GroupAbstractKernelPtr> {
  // nop
};

} // namespace caf

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAF_SERIALIZATION_CAFGROUPABSTRACTKERNELSERIALIZER_H
