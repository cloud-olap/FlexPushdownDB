//
// Created by Yifei Yang on 1/13/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CAFSERIALIZATION_CAFTUPLEKEYELEMENTSERIALIZER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CAFSERIALIZATION_CAFTUPLEKEYELEMENTSERIALIZER_H

#include <fpdb/tuple/TupleKey.h>
#include <fpdb/caf/CAFUtil.h>

using TupleKeyElementPtr = shared_ptr<fpdb::tuple::TupleKeyElement>;

CAF_BEGIN_TYPE_ID_BLOCK(TupleKeyElement, fpdb::caf::CAFUtil::TupleKeyElement_first_custom_type_id)
CAF_ADD_TYPE_ID(TupleKeyElement, (TupleKeyElementPtr))
CAF_ADD_TYPE_ID(TupleKeyElement, (fpdb::tuple::TupleKeyElementWrapper<arrow::Int32Type>))
CAF_ADD_TYPE_ID(TupleKeyElement, (fpdb::tuple::TupleKeyElementWrapper<arrow::Int64Type>))
CAF_ADD_TYPE_ID(TupleKeyElement, (fpdb::tuple::TupleKeyElementWrapper<arrow::DoubleType>))
CAF_ADD_TYPE_ID(TupleKeyElement, (fpdb::tuple::TupleKeyElementWrapper<arrow::BooleanType>))
CAF_ADD_TYPE_ID(TupleKeyElement, (fpdb::tuple::TupleKeyElementWrapper<arrow::Date64Type>))
CAF_ADD_TYPE_ID(TupleKeyElement, (fpdb::tuple::TupleKeyElementWrapper<arrow::StringType>))
CAF_END_TYPE_ID_BLOCK(TupleKeyElement)

namespace caf {

template<>
struct variant_inspector_traits<TupleKeyElementPtr> {
  using value_type = TupleKeyElementPtr;

  // Lists all allowed types and gives them a 0-based index.
  static constexpr type_id_t allowed_types[] = {
          type_id_v<none_t>,
          type_id_v<fpdb::tuple::TupleKeyElementWrapper<arrow::Int32Type>>,
          type_id_v<fpdb::tuple::TupleKeyElementWrapper<arrow::Int64Type>>,
          type_id_v<fpdb::tuple::TupleKeyElementWrapper<arrow::DoubleType>>,
          type_id_v<fpdb::tuple::TupleKeyElementWrapper<arrow::BooleanType>>,
          type_id_v<fpdb::tuple::TupleKeyElementWrapper<arrow::Date64Type>>,
          type_id_v<fpdb::tuple::TupleKeyElementWrapper<arrow::StringType>>
  };

  // Returns which type in allowed_types corresponds to x.
  static auto type_index(const value_type &x) {
    if (!x)
      return 0;
    else if (x->type()->id() == arrow::int32()->id())
      return 1;
    else if (x->type()->id() == arrow::int64()->id())
      return 2;
    else if (x->type()->id() == arrow::float64()->id())
      return 3;
    else if (x->type()->id() == arrow::boolean()->id())
      return 4;
    else if (x->type()->id() == arrow::date64()->id())
      return 5;
    else if (x->type()->id() == arrow::utf8()->id())
      return 6;
    else
      return -1;
  }

  // Applies f to the value of x.
  template<class F>
  static auto visit(F &&f, const value_type &x) {
    switch (type_index(x)) {
      case 1:
        return f(dynamic_cast<fpdb::tuple::TupleKeyElementWrapper<arrow::Int32Type> &>(*x));
      case 2:
        return f(dynamic_cast<fpdb::tuple::TupleKeyElementWrapper<arrow::Int64Type> &>(*x));
      case 3:
        return f(dynamic_cast<fpdb::tuple::TupleKeyElementWrapper<arrow::DoubleType> &>(*x));
      case 4:
        return f(dynamic_cast<fpdb::tuple::TupleKeyElementWrapper<arrow::BooleanType> &>(*x));
      case 5:
        return f(dynamic_cast<fpdb::tuple::TupleKeyElementWrapper<arrow::Date64Type> &>(*x));
      case 6:
        return f(dynamic_cast<fpdb::tuple::TupleKeyElementWrapper<arrow::StringType> &>(*x));
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
      case type_id_v<fpdb::tuple::TupleKeyElementWrapper<arrow::Int32Type>>: {
        auto tmp = fpdb::tuple::TupleKeyElementWrapper<arrow::Int32Type>{};
        continuation(tmp);
        return true;
      }
      case type_id_v<fpdb::tuple::TupleKeyElementWrapper<arrow::Int64Type>>: {
        auto tmp = fpdb::tuple::TupleKeyElementWrapper<arrow::Int64Type>{};
        continuation(tmp);
        return true;
      }
      case type_id_v<fpdb::tuple::TupleKeyElementWrapper<arrow::DoubleType>>: {
        auto tmp = fpdb::tuple::TupleKeyElementWrapper<arrow::DoubleType>{};
        continuation(tmp);
        return true;
      }
      case type_id_v<fpdb::tuple::TupleKeyElementWrapper<arrow::BooleanType>>: {
        auto tmp = fpdb::tuple::TupleKeyElementWrapper<arrow::BooleanType>{};
        continuation(tmp);
        return true;
      }
      case type_id_v<fpdb::tuple::TupleKeyElementWrapper<arrow::Date64Type>>: {
        auto tmp = fpdb::tuple::TupleKeyElementWrapper<arrow::Date64Type>{};
        continuation(tmp);
        return true;
      }
      case type_id_v<fpdb::tuple::TupleKeyElementWrapper<arrow::StringType>>: {
        auto tmp = fpdb::tuple::TupleKeyElementWrapper<arrow::StringType>{};
        continuation(tmp);
        return true;
      }
    }
  }
};

template <>
struct inspector_access<TupleKeyElementPtr> : variant_inspector_access<TupleKeyElementPtr> {
  // nop
};

} // namespace caf

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_CAFSERIALIZATION_CAFTUPLEKEYELEMENTSERIALIZER_H
