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
	valueIndexMap_(std::make_shared<ValueRowMap>()) {}

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

const std::shared_ptr<TupleSet2> &HashTable::getTupleSet() const {
  return tuples_;
}

[[nodiscard]] const std::shared_ptr<ValueRowMap> &HashTable::getValueRowMap() const {
  return valueIndexMap_;
}

std::string HashTable::toString() {

  std::string s;

  s += fmt::format("ValueRowMap:\n");
  for(auto & entry : *valueIndexMap_){
    s+= fmt::format("value: {{{}: {}}}, row: [{}]\n", entry.first->showString(), entry.first->type()->ToString(), entry.second);
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

  for (const std::shared_ptr<Scalar> &element : *joinColumn) {
	valueIndexMap_->emplace(element, rowCounter + rowOffset);
	rowCounter++;
  }

  tuples_->append(tupleSet);

  return {};
}

