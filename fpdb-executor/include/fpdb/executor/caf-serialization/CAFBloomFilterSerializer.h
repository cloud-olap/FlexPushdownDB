//
// Created by Yifei Yang on 11/24/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAF_SERIALIZATION_CAFBLOOMFILTERSERIALIZER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAF_SERIALIZATION_CAFBLOOMFILTERSERIALIZER_H

#include <fpdb/executor/physical/bloomfilter/BloomFilter.h>
#include <fpdb/executor/physical/bloomfilter/ArrowBloomFilter.h>
#include <fpdb/caf/CAFUtil.h>

using namespace fpdb::executor::physical::bloomfilter;
using BloomFilterBasePtr = std::shared_ptr<BloomFilterBase>;

CAF_BEGIN_TYPE_ID_BLOCK(BloomFilter, fpdb::caf::CAFUtil::BloomFilter_first_custom_type_id)
CAF_ADD_TYPE_ID(BloomFilter, (BloomFilterBasePtr))
CAF_ADD_TYPE_ID(BloomFilter, (BloomFilter))
CAF_ADD_TYPE_ID(BloomFilter, (ArrowBloomFilter))
CAF_END_TYPE_ID_BLOCK(BloomFilter)

// Variant-based approach on BloomFilterBasePtr
namespace caf {

template<>
struct variant_inspector_traits<BloomFilterBasePtr> {
  using value_type = BloomFilterBasePtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<BloomFilter>,
          type_id_v<ArrowBloomFilter>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->getType() == BloomFilterType::BLOOM_FILTER)
      return 1;
    else
      return 2;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(dynamic_cast<BloomFilter &>(*x));
      case 2:
        return f(dynamic_cast<ArrowBloomFilter &>(*x));
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
      case type_id_v<BloomFilter>: {
        auto tmp = BloomFilter{};
        continuation(tmp);
        return true;
      }
      case type_id_v<ArrowBloomFilter>: {
        auto tmp = ArrowBloomFilter{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<BloomFilterBasePtr> : variant_inspector_access<BloomFilterBasePtr> {
  // nop
};

} // namespace caf

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CAF_SERIALIZATION_CAFBLOOMFILTERSERIALIZER_H
