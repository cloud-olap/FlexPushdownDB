//
// Created by Yifei Yang on 10/17/22.
//

#ifndef FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_CAF_SERIALIZATION_CAFCACHINGPOLICYSERIALIZER_H
#define FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_CAF_SERIALIZATION_CAFCACHINGPOLICYSERIALIZER_H

#include <fpdb/cache/policy/LRUCachingPolicy.h>
#include <fpdb/cache/policy/LFUCachingPolicy.h>
#include <fpdb/cache/policy/LFUSCachingPolicy.h>
#include <fpdb/cache/policy/WLFUCachingPolicy.h>
#include <fpdb/cache/policy/BeladyCachingPolicy.h>
#include <fpdb/caf/CAFUtil.h>

using namespace fpdb::cache::policy;
using CachingPolicyPtr = std::shared_ptr<CachingPolicy>;

CAF_BEGIN_TYPE_ID_BLOCK(CachingPolicy, fpdb::caf::CAFUtil::CachingPolicy_first_custom_type_id)
CAF_ADD_TYPE_ID(CachingPolicy, (CachingPolicyPtr))
CAF_ADD_TYPE_ID(CachingPolicy, (LRUCachingPolicy))
CAF_ADD_TYPE_ID(CachingPolicy, (LFUCachingPolicy))
CAF_ADD_TYPE_ID(CachingPolicy, (LFUSCachingPolicy))
CAF_ADD_TYPE_ID(CachingPolicy, (WLFUCachingPolicy))
CAF_ADD_TYPE_ID(CachingPolicy, (BeladyCachingPolicy))
CAF_END_TYPE_ID_BLOCK(CachingPolicy)

// Variant-based approach on CachingPolicyPtr
namespace caf {

template<>
struct variant_inspector_traits<CachingPolicyPtr> {
  using value_type = CachingPolicyPtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<LRUCachingPolicy>,
          type_id_v<LFUCachingPolicy>,
          type_id_v<LFUSCachingPolicy>,
          type_id_v<WLFUCachingPolicy>,
          type_id_v<BeladyCachingPolicy>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->getType() == CachingPolicyType::LRU)
      return 1;
    else if (x->getType() == CachingPolicyType::LFU)
      return 2;
    else if (x->getType() == CachingPolicyType::LFUS)
      return 3;
    else if (x->getType() == CachingPolicyType::WLFU)
      return 4;
    else if (x->getType() == CachingPolicyType::BELADY)
      return 5;
    else return -1;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(dynamic_cast<LRUCachingPolicy &>(*x));
      case 2:
        return f(dynamic_cast<LFUCachingPolicy &>(*x));
      case 3:
        return f(dynamic_cast<LFUSCachingPolicy &>(*x));
      case 4:
        return f(dynamic_cast<WLFUCachingPolicy &>(*x));
      case 5:
        return f(dynamic_cast<BeladyCachingPolicy &>(*x));
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
      case type_id_v<LRUCachingPolicy>: {
        auto tmp = LRUCachingPolicy{};
        continuation(tmp);
        return true;
      }
      case type_id_v<LFUCachingPolicy>: {
        auto tmp = LFUCachingPolicy{};
        continuation(tmp);
        return true;
      }
      case type_id_v<LFUSCachingPolicy>: {
        auto tmp = LFUSCachingPolicy{};
        continuation(tmp);
        return true;
      }
      case type_id_v<WLFUCachingPolicy>: {
        auto tmp = WLFUCachingPolicy{};
        continuation(tmp);
        return true;
      }
      case type_id_v<BeladyCachingPolicy>: {
        auto tmp = BeladyCachingPolicy{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<CachingPolicyPtr> : variant_inspector_access<CachingPolicyPtr> {
  // nop
};

} // namespace caf

#endif //FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_CAF_SERIALIZATION_CAFCACHINGPOLICYSERIALIZER_H
