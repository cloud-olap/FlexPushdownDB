//
// Created by matt on 9/4/20.
//

#ifndef FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_ARRAYHELPER_H
#define FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_ARRAYHELPER_H

#include <tl/expected.hpp>
#include <arrow/array.h>

/**
 * Utilities for working with a single Arrow array
 *
 * @tparam ARROW_ARRAY_TYPE
 * @tparam C_TYPE
 */
template<typename ARROW_ARRAY_TYPE, typename C_TYPE = typename ARROW_ARRAY_TYPE::c_type>
class ArrayHelper {
public:
  static tl::expected<C_TYPE, std::string> at(const ARROW_ARRAY_TYPE &array, int index) {
    return array.Value(index);
  }
};

template <typename ARROW_TYPE, typename TRAITS = arrow::TypeTraits<ARROW_TYPE>,
    typename ARROW_SCALAR_TYPE = typename TRAITS::ScalarType,
    typename ARROW_BUILDER_TYPE = typename TRAITS::BuilderType,
    typename ARROW_ARRAY_TYPE = typename TRAITS::ArrayType>
static tl::expected<std::shared_ptr<ARROW_ARRAY_TYPE>, std::string> makeArgh(const ARROW_SCALAR_TYPE &scalar) {
  ARROW_BUILDER_TYPE builder(arrow::default_memory_pool());
  auto res = builder.Append(scalar->value);
  if(!res.ok())
	throw std::runtime_error(res.message());
  std::shared_ptr<ARROW_ARRAY_TYPE> col;
  res = builder.Finish(&col);
  if(!res.ok())
	throw std::runtime_error(res.message());
  return col;
}

/**
 * Specialization on strings, Arrow treats them differently to primitives
 */
template<>
class ArrayHelper<arrow::StringArray, std::string> {
public:

  /**
   * TODO: Maybe this should return a pointer?
   *
   * @param array
   * @param index
   * @return
   */
  static tl::expected<std::string, std::string> at(const arrow::StringArray &array, int index) {
    return array.GetString(index);
  }
};

#endif //FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_ARRAYHELPER_H
