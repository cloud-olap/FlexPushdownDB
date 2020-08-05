//
// Created by matt on 3/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEXFINDERWRAPPER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEXFINDERWRAPPER_H

#include <memory>
#include <utility>
#include <fmt/format.h>

#include "TupleSetIndexFinder.h"
#include "normal/tuple/TupleSetIndex.h"
#include "TupleSetIndexWrapper.h"
#include "normal/tuple/Globals.h"

namespace normal::tuple {

/**
 * A typed tuple set index finder, does the actual work of the finder while allowing the super type
 * to erase the types the finder works on.
 *
 * @tparam CType
 * @tparam ArrowType
 */
template<typename CType, typename ArrowType>
class TupleSetIndexFinderWrapper : public TupleSetIndexFinder {

  using ArrowArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;

public:
  TupleSetIndexFinderWrapper(std::shared_ptr<TupleSetIndexWrapper<CType, ArrowType>> tupleSetIndex,
							 std::shared_ptr<ArrowArrayType> array) :
	  tupleSetIndex_(std::move(tupleSetIndex)),
	  array_(std::move(array)) {}

  std::vector<int64_t> find(int64_t /*rowIndex*/) override {
	// NOOP
  }

private:
  std::shared_ptr<TupleSetIndexWrapper<CType, ArrowType>> tupleSetIndex_;
  std::shared_ptr<ArrowArrayType> array_;

};

template<>
inline std::vector<int64_t> TupleSetIndexFinderWrapper<std::string, ::arrow::StringType>::find(
	int64_t rowIndex) {
  auto value = array_->GetString(rowIndex);
  return tupleSetIndex_->find(value);
}

template<>
inline std::vector<int64_t> TupleSetIndexFinderWrapper<long, ::arrow::Int64Type>::find(
	int64_t rowIndex) {
  auto value = array_->Value(rowIndex);
  return tupleSetIndex_->find(value);
}

class ArraySetIndexFinderBuilder {
public:

  static tl::expected<std::shared_ptr<TupleSetIndexFinder>, std::string>
  make(const std::shared_ptr<TupleSetIndex> &tupleSetIndex, const std::shared_ptr<::arrow::Array> &array) {

	if (tupleSetIndex->type()->id() == ::arrow::StringType::type_id) {

	  auto typedArraySetIndex =
		  std::static_pointer_cast<TupleSetIndexWrapper<std::string, ::arrow::StringType>>(tupleSetIndex);
	  auto typedArray = std::static_pointer_cast<::arrow::StringArray>(array);

	  auto finder =
		  std::make_shared<TupleSetIndexFinderWrapper<std::string, ::arrow::StringType>>(typedArraySetIndex,
																						 typedArray);

	  return finder;
	} else if (tupleSetIndex->type()->id() == ::arrow::Int64Type::type_id) {
	  auto typedArraySetIndex =
		  std::static_pointer_cast<TupleSetIndexWrapper<long, ::arrow::Int64Type>>(tupleSetIndex);
	  auto typedArray = std::static_pointer_cast<::arrow::Int64Array>(array);

	  auto finder =
		  std::make_shared<TupleSetIndexFinderWrapper<long, ::arrow::Int64Type>>(typedArraySetIndex,
																				 typedArray);

	  return finder;
	} else {
	  return tl::make_unexpected(
		  fmt::format("TupleSetIndexFinder not implemented for type '{}'", tupleSetIndex->type()->id()));
	}

  }
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEXFINDERWRAPPER_H
