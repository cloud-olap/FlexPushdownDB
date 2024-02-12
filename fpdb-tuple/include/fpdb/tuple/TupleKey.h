//
// Created by Yifei Yang on 12/8/21.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_TUPLEKEY_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_TUPLEKEY_H

#include <fpdb/util/Util.h>
#include <fpdb/caf/CAFUtil.h>
#include <tl/expected.hpp>
#include <arrow/api.h>
#include <fmt/format.h>
#include <memory>
#include <sstream>

using namespace fpdb::util;
using namespace std;

namespace fpdb::tuple {

class TupleKeyElement {
public:
  virtual ~TupleKeyElement() = default;
  virtual bool equals(const shared_ptr<TupleKeyElement> &other) = 0;
  virtual size_t hash() = 0;
  virtual shared_ptr<arrow::DataType> type() = 0;
  virtual tl::expected<shared_ptr<arrow::Scalar>, string> toScalar() = 0;
  virtual string toString() = 0;
};

template<typename ArrowType>
class TupleKeyElementWrapper : public TupleKeyElement {

using CType = typename arrow::TypeTraits<ArrowType>::CType;

public:
  explicit TupleKeyElementWrapper(CType value) : value(move(value)) {}
  TupleKeyElementWrapper() = default;
  TupleKeyElementWrapper(const TupleKeyElementWrapper&) = default;
  TupleKeyElementWrapper& operator=(const TupleKeyElementWrapper&) = default;

  size_t hash() override {
    return std::hash<CType>()(value);
  };

  bool equals(const shared_ptr<TupleKeyElement> &other) override  {
    return value == static_pointer_cast<TupleKeyElementWrapper<ArrowType>>(other)->value;
  };

  shared_ptr<arrow::DataType> type() override {
    return arrow::TypeTraits<ArrowType>::type_singleton();
  }

  tl::expected<shared_ptr<arrow::Scalar>, string> toScalar() override {
    const auto expScalar = arrow::MakeScalar(type(), value);
    if (!expScalar.ok()) {
      return tl::make_unexpected(expScalar.status().message());
    }
    return expScalar.ValueOrDie();
  }

  string toString() override {
    return to_string(value);
  }

private:
  CType value;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, TupleKeyElementWrapper& tupleKeyElementWrapper) {
    return f.apply(tupleKeyElementWrapper.value);
  }
};

template<>
class TupleKeyElementWrapper<arrow::StringType> : public TupleKeyElement {
public:
  explicit TupleKeyElementWrapper(string value) : value(move(value)) {}
  TupleKeyElementWrapper() = default;
  TupleKeyElementWrapper(const TupleKeyElementWrapper&) = default;
  TupleKeyElementWrapper& operator=(const TupleKeyElementWrapper&) = default;

  size_t hash() override {
    return std::hash<string>()(value);
  };

  bool equals(const shared_ptr<TupleKeyElement> &other) override  {
    return value == static_pointer_cast<TupleKeyElementWrapper<arrow::StringType>>(other)->value;
  };

  shared_ptr<arrow::DataType> type() override {
    return arrow::utf8();
  }

  tl::expected<shared_ptr<arrow::Scalar>, string> toScalar() override {
    return arrow::MakeScalar(value);
  }

  string toString() override {
    return value;
  }

private:
  string value;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, TupleKeyElementWrapper& tupleKeyElementWrapper) {
    return f.apply(tupleKeyElementWrapper.value);
  }
};

class TupleKeyElementBuilder {
public:
  static tl::expected<shared_ptr<TupleKeyElement>, string> make(int64_t r,
                                                                const shared_ptr<arrow::Array> &array) {
    switch (array->type_id()) {
      case arrow::Type::INT16:
        return make_shared<TupleKeyElementWrapper<arrow::Int16Type>>(
                static_pointer_cast<arrow::Int16Array>(array)->Value(r));
      case arrow::Type::INT32:
        return make_shared<TupleKeyElementWrapper<arrow::Int32Type>>(
                static_pointer_cast<arrow::Int32Array>(array)->Value(r));
      case arrow::Type::INT64:
        return make_shared<TupleKeyElementWrapper<arrow::Int64Type>>(
                static_pointer_cast<arrow::Int64Array>(array)->Value(r));
      case arrow::Type::FLOAT:
        return make_shared<TupleKeyElementWrapper<arrow::FloatType>>(
                static_pointer_cast<arrow::FloatArray>(array)->Value(r));
      case arrow::Type::DOUBLE:
        return make_shared<TupleKeyElementWrapper<arrow::DoubleType>>(
                static_pointer_cast<arrow::DoubleArray>(array)->Value(r));
      case arrow::Type::DATE64:
        return make_shared<TupleKeyElementWrapper<arrow::Date64Type>>(
                static_pointer_cast<arrow::Date64Array>(array)->Value(r));
      case arrow::Type::STRING:
        return make_shared<TupleKeyElementWrapper<arrow::StringType>>(
                static_pointer_cast<arrow::StringArray>(array)->GetString(r));
      default: return tl::make_unexpected(fmt::format("Unrecognized type {}", array->type()->name()));
    }
  }
};

class TupleKey {
public:
  explicit TupleKey(vector<shared_ptr<TupleKeyElement>> Elements) : elements_(move(Elements)) {}
  TupleKey() = default;
  TupleKey(const TupleKey&) = default;
  TupleKey& operator=(const TupleKey&) = default;

  size_t hash() {
    vector<size_t> hashes;
    for (const auto &element: elements_) {
      hashes.emplace_back(element->hash());
    }
    return hashCombine(hashes);
  };

  bool operator==(const TupleKey &other) {
    for (size_t i = 0; i < elements_.size(); ++i) {
      if (!elements_[i]->equals(other.elements_[i]))
        return false;
    }
    return true;
  };

  const vector<shared_ptr<TupleKeyElement>> &getElements() const {
    return elements_;
  }

  tl::expected<vector<shared_ptr<arrow::Scalar>>, string> getScalars() const {
    vector<shared_ptr<arrow::Scalar>> scalars;
    for (const auto &element: elements_) {
      const auto &expScalar = element->toScalar();
      if (!expScalar.has_value()) {
        return tl::make_unexpected(expScalar.error());
      }
      scalars.emplace_back(expScalar.value());
    }
    return scalars;
  }

  string toString() const {
    stringstream ss;
    for (uint i = 0; i < elements_.size(); ++i) {
      ss << elements_[i]->toString();
      if (i < elements_.size() - 1) {
        ss << ", ";
      }
    }
    return ss.str();
  }

private:
  vector<shared_ptr<TupleKeyElement>> elements_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, TupleKey& tupleKey) {
    return f.apply(tupleKey.elements_);
  }
};

struct TupleKeyPointerHash {
  inline size_t operator()(const shared_ptr<TupleKey> &key) const {
    return key->hash();
  }
};

struct TupleKeyPointerPredicate {
  inline bool operator()(const shared_ptr<TupleKey> &lhs, const shared_ptr<TupleKey> &rhs) const {
    return *lhs == *rhs;
  }
};

class TupleKeyBuilder {
public:
  static tl::expected<shared_ptr<TupleKey>, string> make(int64_t row,
                                                         const vector<int> &columnIndices,
                                                         const arrow::RecordBatch &recordBatch) {
    vector<shared_ptr<TupleKeyElement>> elements;
    for (const auto &columnIndex: columnIndices) {
      auto expectedTupleKeyElement = TupleKeyElementBuilder::make(row, recordBatch.column(columnIndex));
      if (!expectedTupleKeyElement)
        return tl::make_unexpected(expectedTupleKeyElement.error());
      elements.push_back(expectedTupleKeyElement.value());
    }

    auto groupKey = make_shared<TupleKey>(elements);
    return groupKey;
  }
};

}

using TupleKeyPtr = std::shared_ptr<fpdb::tuple::TupleKey>;

CAF_BEGIN_TYPE_ID_BLOCK(TupleKey, fpdb::caf::CAFUtil::TupleKey_first_custom_type_id)
CAF_ADD_TYPE_ID(TupleKey, (fpdb::tuple::TupleKey))
CAF_END_TYPE_ID_BLOCK(TupleKey)

namespace caf {
template <>
struct inspector_access<TupleKeyPtr> : variant_inspector_access<TupleKeyPtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_TUPLEKEY_H
