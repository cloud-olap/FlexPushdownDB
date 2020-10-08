//
// Created by matt on 29/7/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_ARRAYAPPENDER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_ARRAYAPPENDER_H

#include <arrow/api.h>
#include <tl/expected.hpp>

namespace normal::tuple {

/**
 * Class for building arrays from another array.
 *
 * Mainly just for casting the source array to the appropriate type to access its values, which can
 * be done on construction rather than each append.
 *
 * Subclasses also contain some buffering which seems to be much faster than adding values
 * one by one directly to the destination array.
 */
class ArrayAppender {
public:
  virtual ~ArrayAppender() = default;

  /**
   * Appends value at index in given array to this appender
   *
   * @param array
   * @param i
   */
  virtual inline void
  appendValue(const std::shared_ptr<::arrow::Array> &array, size_t i) = 0;

  /**
   * Appends value at index in given array to this appender, additionally performing a valid check on the given
   * array and index.
   *
   * @param array
   * @param i
   * @return
   */
  virtual inline tl::expected<void, std::string>
  safeAppendValue(const std::shared_ptr<::arrow::Array> &array, size_t i) = 0;

  /**
   * Builds the final array.
   *
   * @return
   */
  virtual tl::expected<std::shared_ptr<arrow::Array>, std::string> finalize() = 0;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_ARRAYAPPENDER_H
