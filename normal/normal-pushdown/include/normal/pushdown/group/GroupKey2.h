//
// Created by matt on 20/10/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUPKEY2_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUPKEY2_H

#include <cstdlib>
#include <memory>

#include <tl/expected.hpp>
#include <utility>
#include <arrow/api.h>
#include <normal/tuple/ArrayAppenderWrapper.h>

namespace normal::pushdown::group {

class GroupKeyElement {
public:
  virtual ~GroupKeyElement() = default;
  virtual bool equals(std::shared_ptr<GroupKeyElement> other) = 0;
  virtual size_t hash() = 0;
  virtual std::string toString() = 0;
};

template<typename ArrowType>
class GroupKeyElementWrapper : public GroupKeyElement {

  using CType = typename arrow::TypeTraits<ArrowType>::CType;

public:
  explicit GroupKeyElementWrapper(CType value) : value(std::move(value)) {}

  size_t hash() override {
	return std::hash<CType>()(value);
  };

  bool equals(std::shared_ptr<GroupKeyElement> other) override  {
	return value == std::static_pointer_cast<GroupKeyElementWrapper<ArrowType>>(other)->value;
  };

  std::string toString() override {
	return std::to_string(value);
  }

private:
  CType value;

};

template<>
class GroupKeyElementWrapper<arrow::StringType> : public GroupKeyElement {
public:
  explicit GroupKeyElementWrapper(std::string value) : value(std::move(value)) {}

  size_t hash() override {
	return std::hash<std::string>()(value);
  };

  bool equals(std::shared_ptr<GroupKeyElement> other) override  {
	return value == std::static_pointer_cast<GroupKeyElementWrapper<arrow::StringType>>(other)->value;
  };

  std::string toString() override {
	return value;
  }

private:
  std::string value;

};

class GroupKeyElementBuilder {
public:
  static tl::expected<std::shared_ptr<GroupKeyElement>, std::string> make(int r,
																		  const std::shared_ptr<arrow::Array> &array) {

	switch (array->type_id()) {

//	case arrow::Type::NA:break;
//	case arrow::Type::BOOL:break;
//	case arrow::Type::UINT8:break;
//	case arrow::Type::INT8:break;
//	case arrow::Type::UINT16:break;
	case arrow::Type::INT16:
	  return std::make_shared<GroupKeyElementWrapper<arrow::Int16Type>>(
		  std::static_pointer_cast<arrow::Int16Array>(array)->Value(r));
//	case arrow::Type::UINT32:break;
	case arrow::Type::INT32:
	  return std::make_shared<GroupKeyElementWrapper<arrow::Int32Type>>(
		  std::static_pointer_cast<arrow::Int32Array>(array)->Value(r));
//	case arrow::Type::UINT64:break;
	case arrow::Type::INT64:
	  return std::make_shared<GroupKeyElementWrapper<arrow::Int64Type>>(
		  std::static_pointer_cast<arrow::Int64Array>(array)->Value(r));
//	case arrow::Type::HALF_FLOAT:break;
	case arrow::Type::FLOAT:
	  return std::make_shared<GroupKeyElementWrapper<arrow::FloatType>>(
		  std::static_pointer_cast<arrow::FloatArray>(array)->Value(r));
	case arrow::Type::DOUBLE:
	  return std::make_shared<GroupKeyElementWrapper<arrow::DoubleType>>(
		  std::static_pointer_cast<arrow::DoubleArray>(array)->Value(r));
	case arrow::Type::STRING:
	  return std::make_shared<GroupKeyElementWrapper<arrow::StringType>>(
		  std::static_pointer_cast<arrow::StringArray>(array)->GetString(r));
//	case arrow::Type::BINARY:break;
//	case arrow::Type::FIXED_SIZE_BINARY:break;
//	case arrow::Type::DATE32:break;
//	case arrow::Type::DATE64:break;
//	case arrow::Type::TIMESTAMP:break;
//	case arrow::Type::TIME32:break;
//	case arrow::Type::TIME64:break;
//	case arrow::Type::INTERVAL:break;
//	case arrow::Type::DECIMAL:break;
//	case arrow::Type::LIST:break;
//	case arrow::Type::STRUCT:break;
//	case arrow::Type::UNION:break;
//	case arrow::Type::DICTIONARY:break;
//	case arrow::Type::MAP:break;
//	case arrow::Type::EXTENSION:break;
//	case arrow::Type::FIXED_SIZE_LIST:break;
//	case arrow::Type::DURATION:break;
//	case arrow::Type::LARGE_STRING:break;
//	case arrow::Type::LARGE_BINARY:break;
//	case arrow::Type::LARGE_LIST:break;
	default: return tl::make_unexpected(fmt::format("Unrecognized type {}", array->type()->name()));
	}
  }
};

class GroupKey2 {
public:
  explicit GroupKey2(std::vector<std::shared_ptr<GroupKeyElement>> Elements) : elements_(std::move(Elements)) {}

  size_t hash() {
	size_t hash = 17;

	for (const auto &element: elements_) {
	  hash = hash * 31 + element->hash();
	}

	return hash;
  };

  bool operator==(const GroupKey2 &other) {
	for (size_t i = 0; i < elements_.size(); ++i) {
	  if (!elements_[i]->equals(other.elements_[i]))
		return false;
	}
	return true;
  };

  [[nodiscard]] const std::vector<std::shared_ptr<GroupKeyElement>> &getElements() const {
	return elements_;
  }

private:
  std::vector<std::shared_ptr<GroupKeyElement>> elements_;

};

struct GroupKey2PointerHash {
  inline size_t operator()(const std::shared_ptr<GroupKey2> &key) const {
	return key->hash();
  }
};

struct GroupKey2PointerPredicate {
  inline bool operator()(const std::shared_ptr<GroupKey2> &lhs, const std::shared_ptr<GroupKey2> &rhs) const {
	return *lhs == *rhs;
  }
};

class GroupKeyBuilder {
public:
  static tl::expected<std::shared_ptr<GroupKey2>, std::string> make(int row,
																	const std::vector<int> &columnIndices,
																	const arrow::RecordBatch &recordBatch) {
	std::vector<std::shared_ptr<GroupKeyElement>> elements;
	for (const auto &columnIndex: columnIndices) {
	  auto expectedGroupKeyElement = GroupKeyElementBuilder::make(row, recordBatch.column(columnIndex));
	  if (!expectedGroupKeyElement)
		return tl::make_unexpected(expectedGroupKeyElement.error());
	  elements.push_back(expectedGroupKeyElement.value());
	}

	auto groupKey = std::make_shared<GroupKey2>(elements);
	return groupKey;
  }
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_GROUP_GROUPKEY2_H
