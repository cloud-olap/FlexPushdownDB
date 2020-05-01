//
// Created by matt on 30/4/20.
//

#include "normal/pushdown/join/HashTable.h"

#include <arrow/array.h>
#include <normal/pushdown/Globals.h>
#include <arrow/pretty_print.h>

using namespace normal::pushdown;
using namespace normal::pushdown::join;

HashTable::HashTable() :
	tuples_(std::make_shared<TupleSet2>()),
	valueIndexMap_(std::make_shared<std::unordered_multimap<std::shared_ptr<Scalar>, long>>()) {}

void HashTable::clear() {
  tuples_->clear();
  valueIndexMap_->clear();
}

void HashTable::merge(const std::shared_ptr<HashTable>& other) {

  long rowOffset = tuples_->numRows();

  // Add the other rows to hashtable, offsetting their row numbers
  for (auto valueIndexMapIterator = other->valueIndexMap_->begin();
	   valueIndexMapIterator != other->valueIndexMap_->end(); valueIndexMapIterator++) {
	valueIndexMap_->emplace(valueIndexMapIterator->first, valueIndexMapIterator->second + rowOffset);
  }

  // Add the other hashtable table to the table
  tuples_->append(other->tuples_);
}

//void HashTable::put(std::string &columnName, std::shared_ptr<arrow::RecordBatch> &batch) {
//
//  long rowOffset = tuples_->numRows();
//
//  // Map the column values to rows in the table
//  auto joinColumn = batch->GetColumnByName(columnName);
//  if (joinColumn == nullptr) {
//	// FIXME
//	throw std::runtime_error("Column '" + columnName + "' does not exist");
//  }
//  auto joinColumnType = joinColumn->type();
//
//  if (joinColumnType->id() == arrow::int64()->id()) {
//
//	auto typedJoinColumn = std::static_pointer_cast<arrow::Int64Array>(joinColumn);
//	for (long r = 0; r < typedJoinColumn->length(); r++) {
//
//	  // FIXME: Is this the best way to use Arrow, get the value out and then make a scalar?
//
//	  auto value = typedJoinColumn->Value(r);
//	  auto valueScalar = arrow::MakeScalar(value);
//	  valueIndexMap_->emplace(valueScalar, r + rowOffset);
//	}
//  } else {
//	// FIXME
//	throw std::runtime_error("Join on column type '" + joinColumnType->ToString() + "' not implemented yet");
//  }
//
//  // Add the batch to the table
//  std::shared_ptr<arrow::Table> batchTable;
//  auto batchVector = {batch};
//  auto res = arrow::Table::FromRecordBatches(batchVector, &batchTable);
//
//  if (table_ == nullptr) {
//	table_ = batchTable;
//  } else {
//	auto tableVector = {table_, batchTable};
//	auto tableResult = arrow::ConcatenateTables(tableVector);
//	if (tableResult.ok()) {
//	  table_ = *tableResult;
//	} else {
//	  // FIXME
//	  throw std::runtime_error(tableResult.status().ToString());
//	}
//  }
//}

const std::shared_ptr<TupleSet2> &HashTable::getTupleSet() const {
  return tuples_;
}

[[nodiscard]] const std::shared_ptr<std::unordered_multimap<std::shared_ptr<Scalar>, long>> &HashTable::getValueRowMap() const {
  return valueIndexMap_;
}

std::string HashTable::toString() {

  std::string s;

  s += fmt::format("ValueRowMap:\n");
  for(auto & entry : *valueIndexMap_){
    s+= fmt::format("value: {{{}: {}}}, rows: [{}]\n", entry.first->showString(), entry.first->type()->ToString(), entry.second);
  }
  s += fmt::format("\n");

  s += fmt::format("Tuples:\n");
  s += tuples_->showString();

  return s;
}

tl::expected<void, std::string> HashTable::put(std::string &columnName, std::shared_ptr<TupleSet2> &tupleSet) {

  // Get the column from the build tuple set we are joining on
  auto expectedJoinColumn = tupleSet->getColumnByName(columnName);
  if (!expectedJoinColumn.has_value()) {
	return tl::make_unexpected(expectedJoinColumn.error());
  }
  auto joinColumn = expectedJoinColumn.value();

  long rowOffset = tuples_->numRows();
  long rowCounter = 0;

  // Map the column values to rows in the table
  for (Column::iterator columnIterator = joinColumn->begin(); columnIterator != joinColumn->end(); columnIterator++) {
	std::shared_ptr<Scalar> value = columnIterator.get();
	valueIndexMap_->emplace(value, rowCounter + rowOffset);
	rowCounter++;
  }

  tuples_->append(tupleSet);

  return {};
}

