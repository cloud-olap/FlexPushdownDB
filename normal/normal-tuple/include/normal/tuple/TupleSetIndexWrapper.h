//
// Created by matt on 1/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEXWRAPPER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEXWRAPPER_H

#include "normal/tuple/TupleSetIndex.h"

#include <utility>
#include <tl/expected.hpp>

#include "normal/tuple/Globals.h"

using namespace normal::tuple;

namespace normal::tuple {

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
					   const std::string& columnName,
					   size_t columnIndex,
					   std::unordered_multimap<CType, int64_t> valueRowMap)
	  : TupleSetIndex(columnName, columnIndex, table),
	  valueRowMap_(std::move(valueRowMap)) {}

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

	size_t tableRow = rowIndexOffset;
	while (recordBatch) {

	  auto array = recordBatch->column(columnIndex);

	  auto typedArray = std::static_pointer_cast<ArrowArrayType>(array);
	  for (int64_t batchRow = 0; batchRow < typedArray->length(); ++batchRow) {
		valueIndexMap.emplace(Value(typedArray, batchRow), tableRow);
		++tableRow;
	  }

	  // Read a batch
	  recordBatchResult = reader.Next();
	  if (!recordBatchResult.ok()) {
		return tl::make_unexpected(recordBatchResult.status().message());
	  }
	  recordBatch = *recordBatchResult;
	}

	assert(valueIndexMap.size() == static_cast<size_t>(table->num_rows()));

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

	assert(valueRowMap_.size() == static_cast<size_t>(table_->num_rows()));

	return {};
  }

  [[nodiscard]] tl::expected<void, std::string> put(const std::shared_ptr<::arrow::Table> &table) override {

  /**
   * need to synchronize schemas (schemas can be the same but in different orders)
   */
  std::shared_ptr<::arrow::Table> newTable;
  auto schema = table_->schema();
  if (!schema->Equals(table->schema())) {
    std::vector<std::shared_ptr<::arrow::ChunkedArray>> newChunkedArrays;
    for (auto const &field: schema->fields()) {
      auto chunkedArray = table->GetColumnByName(field->name());
      if (!chunkedArray) {
        return tl::make_unexpected("Schemas not compatible");
      }
      newChunkedArrays.emplace_back(chunkedArray);
    }
    newTable = ::arrow::Table::Make(schema, newChunkedArrays);
  } else {
    newTable = table;
  }

	int columnIndex = newTable->schema()->GetFieldIndex(columnName_);
	if(columnIndex == -1)
	  return tl::make_unexpected(fmt::format("Cannot add table to TupleSetIndex. Indexed column '{}' not found in given table.", columnName_));

	if((size_t)columnIndex != columnIndex_)
	  return tl::make_unexpected(fmt::format("Cannot add table to TupleSetIndex. Indexed column '{}' has a different position {} to column in index {}.", columnName_, columnIndex, columnIndex_));

	auto expectedValueRowIndexMap = build(columnIndex_, table_->num_rows(), newTable);
	if (!expectedValueRowIndexMap.has_value())
	  return tl::make_unexpected(expectedValueRowIndexMap.error());
	auto valueRowIndexMap = expectedValueRowIndexMap.value();

	valueRowMap_.insert(valueRowIndexMap.begin(), valueRowIndexMap.end());

	auto result = ::arrow::ConcatenateTables({table_, newTable});
	if (!result.ok())
	  return tl::make_unexpected(result.status().message());
	table_ = *result;

	assert(valueRowMap_.size() == static_cast<size_t>(table_->num_rows()));

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

  [[nodiscard]] tl::expected<void, std::string> validate() override {

	::arrow::Result<std::shared_ptr<::arrow::RecordBatch>> recordBatchResult;
	::arrow::Status status;

	std::unordered_multimap<CType, int64_t> valueIndexMap{};

	// Read the table a batch at a time
	::arrow::TableBatchReader reader{*table_};
	reader.set_chunksize(DefaultChunkSize);

	// Read a batch
	recordBatchResult = reader.Next();
	if (!recordBatchResult.ok()) {
	  return tl::make_unexpected(recordBatchResult.status().message());
	}
	auto recordBatch = *recordBatchResult;

	long tableRow = 0;

	while (recordBatch) {

	  auto array = recordBatch->column(columnIndex_);

	  auto typedArray = std::static_pointer_cast<ArrowArrayType>(array);
	  for (int64_t r = 0; r < typedArray->length(); ++r) {

	    auto value = Value(typedArray, r);

	    SPDLOG_DEBUG("Table value at row {} is '{}'", tableRow, value);

		auto indexes = find(value);
		bool found = false;
		for(size_t i = 0;i<indexes.size();++i){
		  SPDLOG_DEBUG("  Index for value '{}' points to row {}", value, indexes[i]);
		  if(indexes[i] == tableRow) {
			SPDLOG_DEBUG("  FOUND!");
			found = true;
		  }
		}
		if(!found) {
		  SPDLOG_DEBUG("  NOT FOUND!");
		  return tl::make_unexpected(fmt::format(
			  "Index invalid. Value found at row {} is '{}' was not found in the index",
			  tableRow,
			  value));
		}

		++tableRow;
	  }

	  // Read a batch
	  recordBatchResult = reader.Next();
	  if (!recordBatchResult.ok()) {
		return tl::make_unexpected(recordBatchResult.status().message());
	  }
	  recordBatch = *recordBatchResult;
	}

	return {};
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
inline long TupleSetIndexWrapper<long, ::arrow::Int32Type>::Value(
	const std::shared_ptr<::arrow::Int32Array> &array,
	int64_t rowIndex) {
  return array->Value(rowIndex);
}

template<>
inline long TupleSetIndexWrapper<long, ::arrow::Int64Type>::Value(
        const std::shared_ptr<::arrow::Int64Array> &array,
        int64_t rowIndex) {
  return array->Value(rowIndex);
}

template<>
inline long TupleSetIndexWrapper<long, ::arrow::DoubleType>::Value(
        const std::shared_ptr<::arrow::DoubleArray> &array,
        int64_t rowIndex) {
  return array->Value(rowIndex);
}

class TupleSetIndexBuilder {
public:
  [[nodiscard]] static tl::expected<std::shared_ptr<TupleSetIndex>, std::string>
  make(const std::shared_ptr<::arrow::Table> &table, const std::string &columnName) {

	int columnIndex = table->schema()->GetFieldIndex(columnName);
	if(columnIndex == -1)
	  return tl::make_unexpected(fmt::format("Cannot make TupleSetIndex. Column '{}' not found in given table.", columnName));

	auto column = table->schema()->field(columnIndex);

	if (column->type()->id() == ::arrow::StringType::type_id) {
	  auto expectedValueRowMap = TupleSetIndexWrapper<std::string, ::arrow::StringType>::build(columnIndex, 0, table);
	  if(!expectedValueRowMap) return tl::make_unexpected(expectedValueRowMap.error());
	  return std::make_shared<TupleSetIndexWrapper<std::string, ::arrow::StringType>>(table, columnName, columnIndex, expectedValueRowMap.value());
	} else if (column->type()->id() == ::arrow::Int32Type::type_id) {
	  auto expectedValueRowMap = TupleSetIndexWrapper<long, ::arrow::Int32Type>::build(columnIndex, 0, table);
	  if(!expectedValueRowMap) return tl::make_unexpected(expectedValueRowMap.error());
	  return std::make_shared<TupleSetIndexWrapper<long, ::arrow::Int32Type>>(table, columnName, columnIndex, expectedValueRowMap.value());
	} else if (column->type()->id() == ::arrow::Int64Type::type_id) {
    auto expectedValueRowMap = TupleSetIndexWrapper<long, ::arrow::Int64Type>::build(columnIndex, 0, table);
    if(!expectedValueRowMap) return tl::make_unexpected(expectedValueRowMap.error());
    return std::make_shared<TupleSetIndexWrapper<long, ::arrow::Int64Type>>(table, columnName, columnIndex, expectedValueRowMap.value());
  } else if (column->type()->id() == ::arrow::DoubleType::type_id) {
    auto expectedValueRowMap = TupleSetIndexWrapper<long, ::arrow::DoubleType>::build(columnIndex, 0, table);
    if(!expectedValueRowMap) return tl::make_unexpected(expectedValueRowMap.error());
    return std::make_shared<TupleSetIndexWrapper<long, ::arrow::DoubleType>>(table, columnName, columnIndex, expectedValueRowMap.value());
  } else {
	  return tl::make_unexpected(
		  fmt::format("TupleSetIndex not implemented for type '{}'", column->type()->name()));
	}
  }
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEXWRAPPER_H
