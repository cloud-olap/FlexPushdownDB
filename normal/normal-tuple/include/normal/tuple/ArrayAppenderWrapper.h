//
// Created by matt on 13/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_ARRAYAPPENDERWRAPPER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_ARRAYAPPENDERWRAPPER_H

#include <utility>

#include <tl/expected.hpp>

#include "normal/tuple/ArrayAppender.h"
#include "normal/tuple/Globals.h"

namespace normal::tuple {

template<typename CType, typename ArrowType>
class ArrayAppenderWrapper : public ArrayAppender {

  using ArrowArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;
  using ArrowBuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;

public:
  explicit ArrayAppenderWrapper(size_t expectedSize = 0) :
	  ArrayAppender() {
	buffer_.reserve(expectedSize);
	builder_ = std::make_shared<ArrowBuilderType>();
  }

  static std::shared_ptr<ArrayAppenderWrapper> make(size_t expectedSize = 0) {
	return std::make_shared<ArrayAppenderWrapper>(expectedSize);
  }

  void appendValue(const std::shared_ptr<::arrow::Array> &array, int64_t i) override {
	buffer_.emplace_back(std::static_pointer_cast<ArrowArrayType>(array)->Value(i));
  }

  /**
   * Need this to compile on Mac, not sure why???
   *
   * @param builder
   * @param buffer
   * @return
   */
  ::arrow::Status strangeProblem(const std::shared_ptr<ArrowBuilderType> &builder, const std::vector<CType> &buffer);

  tl::expected<std::shared_ptr<arrow::Array>, std::string> finalize() override {
	::arrow::Status status;
	std::shared_ptr<ArrowArrayType> array;

	buffer_.shrink_to_fit();

	status = strangeProblem(builder_, buffer_);
	if (!status.ok()) {
	  return tl::make_unexpected(status.message());
	}

	status = builder_->Finish(&array);
	if (!status.ok()) {
	  return tl::make_unexpected(status.message());
	}

	buffer_.clear();

	return array;
  }

private:
  std::vector<CType> buffer_;
  std::shared_ptr<ArrowBuilderType> builder_;

};

template<>
void ArrayAppenderWrapper<std::string, ::arrow::StringType>::appendValue(const std::shared_ptr<::arrow::Array> &array, int64_t i);

template<>
inline ::arrow::Status ArrayAppenderWrapper<int, ::arrow::Int32Type>::strangeProblem(const std::shared_ptr<::arrow::Int32Builder> &builder, const std::vector<int> &buffer){
  return builder->AppendValues(buffer);
}

template<>
inline ::arrow::Status ArrayAppenderWrapper<long, ::arrow::Int64Type>::strangeProblem(const std::shared_ptr<::arrow::Int64Builder> &builder, const std::vector<long> &buffer){
  return builder->AppendValues(buffer);
}

template<>
inline ::arrow::Status ArrayAppenderWrapper<std::string, ::arrow::StringType>::strangeProblem(const std::shared_ptr<::arrow::StringBuilder> &builder, const std::vector<std::string> &buffer){
  return builder->AppendValues(buffer);
}

class ArrayAppenderBuilder {
public:
  static tl::expected<std::shared_ptr<ArrayAppender>, std::string>
  make(const std::shared_ptr<::arrow::DataType> &type, size_t expectedSize = 0) {
	if (type->id() == ::arrow::StringType::type_id) {
	  return ArrayAppenderWrapper<std::string, ::arrow::StringType>::make(expectedSize);
	}  else if (type->id() == ::arrow::Int32Type::type_id) {
	  return ArrayAppenderWrapper<int, ::arrow::Int32Type>::make(expectedSize);
	}else if (type->id() == ::arrow::Int64Type::type_id) {
	  return ArrayAppenderWrapper<long, ::arrow::Int64Type>::make(expectedSize);
	} else {
	  return tl::make_unexpected(
		  fmt::format("ArrayAppender not implemented for type '{}'", type->name()));
	}
  }
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_ARRAYAPPENDERWRAPPER_H
