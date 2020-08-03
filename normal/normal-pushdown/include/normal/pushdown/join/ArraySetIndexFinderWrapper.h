//
// Created by matt on 3/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_ARRAYSETINDEXFINDERWRAPPER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_ARRAYSETINDEXFINDERWRAPPER_H

#include <memory>
#include <utility>
#include <fmt/format.h>

#include "ArraySetIndexFinder.h"
#include "ArraySetIndex.h"
#include "TypedArraySetIndex.h"
#include "normal/pushdown/Globals.h"

template<typename CType, typename ArrowType>
class ArraySetIndexFinderWrapper : public ArraySetIndexFinder {
  using ArrowArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;
public:
  ArraySetIndexFinderWrapper(std::shared_ptr<TypedArraySetIndex<CType, ArrowType>> ArraySetIndex,
							 std::shared_ptr<ArrowArrayType> Array)
	  : arraySetIndex_(std::move(ArraySetIndex)), array_(std::move(Array)) {}

  std::vector<int64_t> find(int64_t i) override {
	auto value = array_->GetView(i);
	return arraySetIndex_->find(value);
  }
private:
  std::shared_ptr<TypedArraySetIndex<CType, ArrowType>> arraySetIndex_;
  std::shared_ptr<ArrowArrayType> array_;
};

class ArraySetIndexFinderBuilder {
public:
  static tl::expected<std::shared_ptr<ArraySetIndexFinder>,
					  std::string> make(const std::shared_ptr<ArraySetIndex> &arraySetIndex,
										const std::shared_ptr<::arrow::Array> &array) {
	if (arraySetIndex->type()->id() == ::arrow::StringType::type_id) {
	  auto typedArraySetIndex = std::static_pointer_cast<TypedArraySetIndex<std::string , ::arrow::StringType>>(arraySetIndex);
	  auto typedArray = std::static_pointer_cast<::arrow::StringArray>(array);
	  auto finder =
		  std::make_shared<ArraySetIndexFinderWrapper<std::string, ::arrow::StringType>>(typedArraySetIndex, typedArray);
	  return finder;
	} else {
	  return tl::make_unexpected(fmt::format("ArraySetIndexFinder not implemented for type '{}'",
											 arraySetIndex->type()->id()));
	}
  }
};

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_ARRAYSETINDEXFINDERWRAPPER_H
