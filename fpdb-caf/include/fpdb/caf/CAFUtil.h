//
// Created by Yifei Yang on 1/12/22.
//

#ifndef FPDB_FPDB_CAF_INCLUDE_FPDB_CAF_CAFUTIL_H
#define FPDB_FPDB_CAF_INCLUDE_FPDB_CAF_CAFUTIL_H

#include <caf/all.hpp>

namespace fpdb::caf {

class CAFUtil {

public:
  inline static constexpr ::caf::type_id_t SegmentCacheActor_first_custom_type_id = ::caf::first_custom_type_id;
  inline static constexpr ::caf::type_id_t Envelope_first_custom_type_id = ::caf::first_custom_type_id + 100;
  inline static constexpr ::caf::type_id_t POpActor_first_custom_type_id = ::caf::first_custom_type_id + 200;
  inline static constexpr ::caf::type_id_t POpActor2_first_custom_type_id = ::caf::first_custom_type_id + 300;
  inline static constexpr ::caf::type_id_t CollatePOp2_first_custom_type_id = ::caf::first_custom_type_id + 400;
  inline static constexpr ::caf::type_id_t FileScanPOp2_first_custom_type_id = ::caf::first_custom_type_id + 500;
  inline static constexpr ::caf::type_id_t TupleSet_first_custom_type_id = ::caf::first_custom_type_id + 600;
  inline static constexpr ::caf::type_id_t Message_first_custom_type_id = ::caf::first_custom_type_id + 700;
  inline static constexpr ::caf::type_id_t SegmentKey_first_custom_type_id = ::caf::first_custom_type_id + 800;
  inline static constexpr ::caf::type_id_t Partition_first_custom_type_id = ::caf::first_custom_type_id + 900;
  inline static constexpr ::caf::type_id_t SegmentMetadata_first_custom_type_id = ::caf::first_custom_type_id + 1000;
  inline static constexpr ::caf::type_id_t SegmentData_first_custom_type_id = ::caf::first_custom_type_id + 1100;
  inline static constexpr ::caf::type_id_t Column_first_custom_type_id = ::caf::first_custom_type_id + 1200;
  inline static constexpr ::caf::type_id_t TupleSetIndex_first_custom_type_id = ::caf::first_custom_type_id + 1300;
  inline static constexpr ::caf::type_id_t TupleKey_first_custom_type_id = ::caf::first_custom_type_id + 1400;
  inline static constexpr ::caf::type_id_t TupleKeyElement_first_custom_type_id = ::caf::first_custom_type_id + 1500;
  inline static constexpr ::caf::type_id_t POp_first_custom_type_id = ::caf::first_custom_type_id + 1600;
  inline static constexpr ::caf::type_id_t Scalar_first_custom_type_id = ::caf::first_custom_type_id + 1700;
  inline static constexpr ::caf::type_id_t AggregateFunction_first_custom_type_id = ::caf::first_custom_type_id + 1800;
  inline static constexpr ::caf::type_id_t Expression_first_custom_type_id = ::caf::first_custom_type_id + 1900;
  inline static constexpr ::caf::type_id_t POpContext_first_custom_type_id = ::caf::first_custom_type_id + 2000;
  inline static constexpr ::caf::type_id_t Table_first_custom_type_id = ::caf::first_custom_type_id + 2100;
  inline static constexpr ::caf::type_id_t FileFormat_first_custom_type_id = ::caf::first_custom_type_id + 2200;
  inline static constexpr ::caf::type_id_t HashJoinProbeAbstractKernel_first_custom_type_id = ::caf::first_custom_type_id + 2300;
  inline static constexpr ::caf::type_id_t AggregateResult_first_custom_type_id = ::caf::first_custom_type_id + 2400;
  inline static constexpr ::caf::type_id_t FileScanKernel_first_custom_type_id = ::caf::first_custom_type_id + 2500;
  inline static constexpr ::caf::type_id_t PhysicalPlan_first_custom_type_id = ::caf::first_custom_type_id + 2600;
  inline static constexpr ::caf::type_id_t BloomFilter_first_custom_type_id = ::caf::first_custom_type_id + 2700;
  inline static constexpr ::caf::type_id_t ObjStoreConnector_first_custom_type_id = ::caf::first_custom_type_id + 2800;
  inline static constexpr ::caf::type_id_t GroupAbstractKernel_first_custom_type_id = ::caf::first_custom_type_id + 2900;
  inline static constexpr ::caf::type_id_t FPDBStoreBloomFilterUseInfo_first_custom_type_id = ::caf::first_custom_type_id + 3000;
  inline static constexpr ::caf::type_id_t Mode_first_custom_type_id = ::caf::first_custom_type_id + 3100;
  inline static constexpr ::caf::type_id_t CachingPolicy_first_custom_type_id = ::caf::first_custom_type_id + 3200;
  inline static constexpr ::caf::type_id_t BloomFilterCreateAbstractKernel_first_custom_type_id = ::caf::first_custom_type_id + 3300;

};

}

// A template to serialize any shared_ptr
namespace caf {
template <class T>
struct variant_inspector_traits<std::shared_ptr<T>> {
  using value_type = std::shared_ptr<T>;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<T>,
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type& x) {
    if (!x)
      return 0;
    else
      return 1;
  }

  // Applies f to the value of x.
  template <class F>
  static auto visit(F&& f, const value_type& x) {
    switch (type_index(x)) {
      case 0: {
        none_t dummy;
        return f(dummy);
      }
      default:{
        auto a = f(static_cast<T&>(*x));
        return a;
      }
    }
  }

  // Assigns a value to x.
  template <class U>
  static void assign(value_type& x, U value) {
    if constexpr (std::is_same<U, none_t>::value)
      x.reset();
    else
      x = std::make_shared<U>(std::move(value));
  }

  // Create a default-constructed object for `type` and then call the
  // continuation with the temporary object to perform remaining load steps.
  template <class F>
  static bool load(type_id_t type, F continuation) {
    switch (type) {
      default:
        return false;
      case type_id_v<none_t>: {
        none_t dummy;
        continuation(dummy);
        return true;
      }
      case type_id_v<T>: {
        auto tmp = T{};
        continuation(tmp);
        return true;
      }
    }
  }
};
} // namespace caf


#endif //FPDB_FPDB_CAF_INCLUDE_FPDB_CAF_CAFUTIL_H
