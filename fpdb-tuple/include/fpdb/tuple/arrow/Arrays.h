//
// Created by matt on 3/4/20.
//

#ifndef FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_ARRAYS_H
#define FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_ARRAYS_H

#include <vector>

#include <arrow/api.h>
#include <tl/expected.hpp>
#include "fpdb/tuple/arrow/ArrayHelper.h"

/**
 * Arrow utility methods
 */
class Arrays {
public:

  /**
   * Creates a typed arrow array from the given vector
   *
   * @tparam TYPE
   * @tparam C_TYPE
   */
  template<typename TYPE, typename C_TYPE = typename TYPE::c_type>
  static tl::expected<std::shared_ptr<arrow::Array>, std::string> make(std::vector<C_TYPE> values) {

    std::shared_ptr<arrow::Array> array;

    arrow::Status result;

    // Create the builder for the given vector type
    std::unique_ptr<arrow::ArrayBuilder> builder_ptr;
    result = arrow::MakeBuilder(arrow::default_memory_pool(), arrow::TypeTraits<TYPE>::type_singleton(), &builder_ptr);
    if(!result.ok())
      return tl::make_unexpected(result.ToString());
    auto &builder = dynamic_cast<typename arrow::TypeTraits<TYPE>::BuilderType &>(*builder_ptr);

    // Add the data
    for (size_t i = 0; i < values.size(); ++i) {
      result = builder.Append(values[i]);
      if(!result.ok())
        return tl::make_unexpected(result.ToString());
    }
    result = builder.Finish(&array);
    if(!result.ok())
      return tl::make_unexpected(result.ToString());

    return array;
  }
};

#endif //FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_ARRAYS_H
