//
// Created by Yifei Yang on 1/16/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAFSERIALIZATION_CAFHASHJOINPROBEABSTRACTKERNELSERIALIZER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAFSERIALIZATION_CAFHASHJOINPROBEABSTRACTKERNELSERIALIZER_H

#include <fpdb/executor/physical/join/hashjoin/HashJoinProbeKernel.h>
#include <fpdb/executor/physical/join/hashjoin/HashSemiJoinProbeKernel.h>
#include <fpdb/caf/CAFUtil.h>

using namespace fpdb::executor::physical::join;
using HashJoinProbeAbstractKernelPtr = std::shared_ptr<HashJoinProbeAbstractKernel>;

CAF_BEGIN_TYPE_ID_BLOCK(HashJoinProbeAbstractKernel, fpdb::caf::CAFUtil::HashJoinProbeAbstractKernel_first_custom_type_id)
CAF_ADD_TYPE_ID(HashJoinProbeAbstractKernel, (HashJoinProbeAbstractKernelPtr))
CAF_ADD_TYPE_ID(HashJoinProbeAbstractKernel, (HashJoinProbeKernel))
CAF_ADD_TYPE_ID(HashJoinProbeAbstractKernel, (HashSemiJoinProbeKernel))
CAF_END_TYPE_ID_BLOCK(HashJoinProbeAbstractKernel)

// Variant-based approach on HashJoinProbeAbstractKernelPtr
namespace caf {

template<>
struct variant_inspector_traits<HashJoinProbeAbstractKernelPtr> {
  using value_type = HashJoinProbeAbstractKernelPtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<HashJoinProbeKernel>,
          type_id_v<HashSemiJoinProbeKernel>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->isSemi() == false)
      return 1;
    else
      return 2;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(dynamic_cast<HashJoinProbeKernel &>(*x));
      case 2:
        return f(dynamic_cast<HashSemiJoinProbeKernel &>(*x));
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
      case type_id_v<HashJoinProbeKernel>: {
        auto tmp = HashJoinProbeKernel{};
        continuation(tmp);
        return true;
      }
      case type_id_v<HashSemiJoinProbeKernel>: {
        auto tmp = HashSemiJoinProbeKernel{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<HashJoinProbeAbstractKernelPtr> : variant_inspector_access<HashJoinProbeAbstractKernelPtr> {
  // nop
};

} // namespace caf

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAFSERIALIZATION_CAFHASHJOINPROBEABSTRACTKERNELSERIALIZER_H
