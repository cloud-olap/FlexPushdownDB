//
// Created by matt on 9/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_ARRAYHELPER_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_ARRAYHELPER_H

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

/**
 * Specialization on strings, Arrow treats them differently yo primitives
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

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_ARRAYHELPER_H
