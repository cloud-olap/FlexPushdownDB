//
// Created by matt on 3/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_ARRAYS_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_ARRAYS_H

#include <vector>
#include <arrow/api.h>

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
  static std::shared_ptr<arrow::Array> make(std::vector<C_TYPE> values) {

    std::shared_ptr<arrow::Array> array;

    // Create the builder for the given vector type
    std::unique_ptr<arrow::ArrayBuilder> builder_ptr;
    arrow::MakeBuilder(arrow::default_memory_pool(), arrow::TypeTraits<TYPE>::type_singleton(), &builder_ptr);
    auto &builder = dynamic_cast<typename arrow::TypeTraits<TYPE>::BuilderType &>(*builder_ptr);

    // Add the data
    for (size_t i = 0; i < values.size(); ++i) {
      builder.Append(values[i]);
    }
    builder.Finish(&array);

    return array;
  }
};

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_ARRAYS_H
