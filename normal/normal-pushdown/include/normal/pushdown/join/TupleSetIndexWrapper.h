//
// Created by matt on 1/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_TUPLESETINDEXWRAPPER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_TUPLESETINDEXWRAPPER_H

#include "TupleSetIndex.h"

#include <utility>
#include <tl/expected.hpp>

#include "normal/tuple/Globals.h"

using namespace normal::tuple;

namespace normal::pushdown::join {

/**
 * A typed tuple set index, does the actual work of the tuple set index while allowing the super type
 * to erase the types the index works on.
 *
 * @tparam CType
 * @tparam ArrowType
 */
template<typename CType, typename ArrowType>
class TupleSetIndexWrapper : public TupleSetIndex {
public:

  using ArrowArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;

  TupleSetIndexWrapper(const std::shared_ptr<::arrow::Table> &table,
					   size_t columnIndex,
					   std::unordered_multimap<CType, int64_t> valueRowMap)
	  : TupleSetIndex(columnIndex, table), valueRowMap_(std::move(valueRowMap)) {}

  static std::shared_ptr<TupleSetIndexWrapper> make(const std::shared_ptr<::arrow::Table> &table, size_t columnIndex) {
	assert(table->schema()->field(columnIndex)->type()->id() == (::arrow::TypeTraits<ArrowType>::type_singleton()->id()));
	auto valueRowMap = build(columnIndex, 0, table);
	return std::make_shared<TupleSetIndexWrapper>(table, columnIndex, valueRowMap.value());
  }

  static CType Value(const std::shared_ptr<ArrowArrayType> &/*array*/, int64_t /*rowIndex*/) {
	// NOOP
  }

  [[nodiscard]] static tl::expected<std::unordered_multimap<CType, int64_t>, std::string> build(size_t columnIndex,
																								int64_t rowIndexOffset,
																								const std::shared_ptr<::arrow::Table> &table) {

	::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> recordBatchResult;
	::arrow::Status status;

	std::unordered_multimap<CType, int64_t> valueIndexMap{};

	// Read the table a batch at a time
	::arrow::TableBatchReader reader{*table};
	reader.set_chunksize(DefaultChunkSize);

	// Read a batch
	recordBatchResult = reader.Next();
	if (!recordBatchResult.ok()) {
	  return tl::make_unexpected(recordBatchResult.status().message());
	}
	auto recordBatch = *recordBatchResult;

	while (recordBatch) {

	  auto array = recordBatch->column(columnIndex);

	  auto typedArray = std::static_pointer_cast<ArrowArrayType>(array);
	  for (int64_t r = 0; r < typedArray->length(); ++r) {
		valueIndexMap.emplace(Value(typedArray, r), r + rowIndexOffset);
	  }

	  // Read a batch
	  recordBatchResult = reader.Next();
	  if (!recordBatchResult.ok()) {
		return tl::make_unexpected(recordBatchResult.status().message());
	  }
	  recordBatch = *recordBatchResult;
	}

	return valueIndexMap;
  }

  [[nodiscard]] tl::expected<void, std::string> merge(const std::shared_ptr<TupleSetIndex> &other) override {

	auto typedOther = std::static_pointer_cast<TupleSetIndexWrapper<CType, ArrowType>>(other);

	long rowOffset = table_->num_rows();

	// Add the other rows to hashtable, offsetting their row numbers
	for (auto valueIndexMapIterator = typedOther->valueRowMap_.begin();
		 valueIndexMapIterator != typedOther->valueRowMap_.end(); valueIndexMapIterator++) {
	  valueRowMap_.emplace(valueIndexMapIterator->first, valueIndexMapIterator->second + rowOffset);
	}

	// Add the other hashtable table to the table
	auto appendResult = ::arrow::ConcatenateTables({table_, typedOther->table_});
	if (!appendResult.ok()) {
	  return tl::make_unexpected(appendResult.status().message());
	}
	table_ = *appendResult;

	return {};
  }

  [[nodiscard]] tl::expected<void, std::string> put(const std::shared_ptr<::arrow::Table> &table) override {

	auto expectedValueIndexMap = build(columnIndex_, table_->num_rows(), table);
	if (!expectedValueIndexMap.has_value())
	  return tl::make_unexpected(expectedValueIndexMap.error());
	valueRowMap_ = expectedValueIndexMap.value();

	auto result = ::arrow::ConcatenateTables({table_, table});
	if (!result.ok())
	  return tl::make_unexpected(result.status().message());
	table_ = *result;

	return {};
  }

  std::vector<int64_t> find(CType value) {
	std::vector<int64_t> rowIndexes;
	auto range = valueRowMap_.equal_range(value);
	for (auto it = range.first; it != range.second; ++it) {
	  rowIndexes.emplace_back(it->second);
	}
	return rowIndexes;
  }

  std::string toString() override {
	std::string s;
	for (const auto &valueRowIndex: valueRowMap_) {
	  s += fmt::format("{} : {}\n", valueRowIndex.first, valueRowIndex.second);
	}
	return s;
  }

private:
  std::unordered_multimap<CType, int64_t> valueRowMap_;

};

template<>
inline std::string TupleSetIndexWrapper<std::string, ::arrow::StringType>::Value(
	const std::shared_ptr<::arrow::StringArray> &array,
	int64_t rowIndex) {
  return array->GetString(rowIndex);
}

template<>
inline long TupleSetIndexWrapper<long, ::arrow::Int64Type>::Value(
	const std::shared_ptr<::arrow::Int64Array> &array,
	int64_t rowIndex) {
  return array->Value(rowIndex);
}

class TupleSetIndexBuilder {
public:
  static tl::expected<std::shared_ptr<TupleSetIndex>, std::string>
  make(const std::shared_ptr<::arrow::Table> &table, const std::string &columnName) {

	size_t columnIndex = table->schema()->GetFieldIndex(columnName);
	auto column = table->schema()->field(columnIndex);

	if (column->type()->id() == ::arrow::StringType::type_id) {
	  return TupleSetIndexWrapper<std::string, ::arrow::StringType>::make(table, columnIndex);
	} else if (column->type()->id() == ::arrow::Int64Type::type_id) {
	  return TupleSetIndexWrapper<long, ::arrow::Int64Type>::make(table, columnIndex);
	} else {
	  return tl::make_unexpected(
		  fmt::format("TupleSetIndex not implemented for type '{}'", column->type()->id()));
	}
  }
};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_TUPLESETINDEXWRAPPER_H
