//
// Created by matt on 1/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_TYPEDARRAYSETINDEX_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_TYPEDARRAYSETINDEX_H

#include "ArraySetIndex.h"

#include <utility>
#include <tl/expected.hpp>

#include "normal/tuple/Globals.h"

using namespace normal::tuple;

template<typename CType, typename ArrowType>
class TypedArraySetIndex : public ArraySetIndex {
public:

  using ArrowArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;

  TypedArraySetIndex(const std::shared_ptr<::arrow::Table> &Table,
					 size_t arrayPos,
					 std::unordered_multimap<CType, int64_t> ValueIndexMap)
	  : ArraySetIndex(arrayPos, Table), valueIndexMap_(std::move(ValueIndexMap)) {}

  static std::shared_ptr<TypedArraySetIndex> make(const std::shared_ptr<::arrow::Table> &Table, size_t arrayPos) {
	assert(Table->schema()->field(arrayPos)->type()->id() == (::arrow::TypeTraits<ArrowType>::type_singleton()->id()));
	auto valueIndexMap = build(arrayPos, 0, Table);
	return std::make_shared<TypedArraySetIndex>(Table, arrayPos, valueIndexMap.value());
  }

  static tl::expected<std::unordered_multimap<CType, int64_t>, std::string> build(size_t arrayPos,
																			 int64_t rowNumOffset,
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

	  auto array = recordBatch->column(arrayPos);

	  auto typedArray = std::static_pointer_cast<ArrowArrayType>(array);
	  for (int64_t r = 0; r < typedArray->length(); ++r) {
		valueIndexMap.emplace(typedArray->GetString(r), r + rowNumOffset);
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

  tl::expected<void, std::string> merge(const std::shared_ptr<ArraySetIndex> &other) override {

    auto typedOther = std::static_pointer_cast<TypedArraySetIndex<CType, ArrowType>>(other);

	long rowOffset = table_->num_rows();

	// Add the other rows to hashtable, offsetting their row numbers
	for (auto valueIndexMapIterator = typedOther->valueIndexMap_.begin();
		 valueIndexMapIterator != typedOther->valueIndexMap_.end(); valueIndexMapIterator++) {
	  valueIndexMap_.emplace(valueIndexMapIterator->first, valueIndexMapIterator->second + rowOffset);
	}

	// Add the other hashtable table to the table
	auto appendResult = ::arrow::ConcatenateTables({table_, typedOther->table_});
	if(!appendResult.ok()){
	  return tl::make_unexpected(appendResult.status().message());
	}
	table_ = *appendResult;

	return {};
  }

  tl::expected<void, std::string> put(const std::shared_ptr<::arrow::Table> &table) override {

	auto expectedValueIndexMap = build(arrayPos_, table_->num_rows(), table);
	if (!expectedValueIndexMap.has_value())
	  return tl::make_unexpected(expectedValueIndexMap.error());
	valueIndexMap_ = expectedValueIndexMap.value();

	auto result = ::arrow::ConcatenateTables({table_, table});
	if(!result.ok())
	  return tl::make_unexpected(result.status().message());
	table_ = *result;

	return {};
  }

  std::vector<int64_t> find(CType value) {
	std::vector<int64_t> indexes;
	auto range = valueIndexMap_.equal_range(value);
	for(auto it = range.first; it != range.second; ++it){
	  indexes.emplace_back(it->second);
	}
	return indexes;
  }

  std::string toString() override {
    std::string s;
	for(const auto &x: valueIndexMap_){
	  s += fmt::format("{} : {}\n", std::string(x.first), x.second);
	}
	return s;
  }

private:
  std::unordered_multimap<CType, int64_t> valueIndexMap_;

};

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_TYPEDARRAYSETINDEX_H
