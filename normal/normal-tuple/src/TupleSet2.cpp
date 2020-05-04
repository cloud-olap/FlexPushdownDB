//
// Created by matt on 1/5/20.
//

#include "normal/tuple/TupleSet2.h"

#include <utility>

#include <arrow/api.h>
#include <arrow/pretty_print.h>

using namespace normal::tuple;

TupleSet2::TupleSet2() :
	table_(std::nullopt) {
}

TupleSet2::TupleSet2(std::shared_ptr<::arrow::Table> arrowTable) :
	table_(std::optional(std::move(arrowTable))) {
}

std::shared_ptr<TupleSet2> TupleSet2::create(const std::shared_ptr<normal::core::TupleSet> &tuples) {
  return std::make_shared<TupleSet2>(tuples->table());
}

void TupleSet2::clear() {
  table_ = std::nullopt;
}

long TupleSet2::numRows() {
  if (table_)
	return table_.value()->num_rows();
  else
	return 0;
}

tl::expected<std::shared_ptr<TupleSet2>,
			 std::string> TupleSet2::concatenate(const std::vector<std::shared_ptr<TupleSet2>> &tupleSets) {

  // Create a vector of arrow tables to concatenate
  std::vector<std::shared_ptr<::arrow::Table>> tableVector = tupleSetVectorToArrowTableVector(tupleSets);

  // Concatenate
  auto tableResult = arrow::ConcatenateTables(tableVector);
  if (tableResult.ok()) {
	return std::make_shared<TupleSet2>(*tableResult);
  } else {
	return tl::make_unexpected(tableResult.status().ToString());
  }
}

std::vector<std::shared_ptr<arrow::Table>> TupleSet2::tupleSetVectorToArrowTableVector(const std::vector<std::shared_ptr<
	TupleSet2>> &tupleSets) {

  std::vector<std::shared_ptr<arrow::Table>> tableVector;
  tableVector.reserve(tupleSets.size());
  for (const auto &tupleSet: tupleSets) {
	if (tupleSet->table_.has_value())
	  tableVector.push_back(tupleSet->table_.value());
  }

  return tableVector;
}

tl::expected<void, std::string> TupleSet2::append(const std::vector<std::shared_ptr<TupleSet2>> &tupleSets) {
  auto tupleSetVector = std::vector{shared_from_this()};
  tupleSetVector.insert(tupleSetVector.end(), tupleSets.begin(), tupleSets.end());
  auto expected = concatenate(tupleSetVector);
  if (!expected.has_value())
	return tl::make_unexpected(expected.error());
  else {
	this->table_ = expected.value()->table_;
	return {}; // TODO: This seems to be how to return a void expected AFAICT, verify?
  }
}

tl::expected<void, std::string> TupleSet2::append(const std::shared_ptr<TupleSet2> &tupleSet) {
  auto tupleSetVector = std::vector{shared_from_this(), tupleSet};
  return append(tupleSetVector);
}

std::string TupleSet2::showString() {

  if(table_.has_value()) {
	auto ss = std::stringstream();
	arrow::Status arrowStatus = ::arrow::PrettyPrint(*table_.value(), 0, &ss);

	if (!arrowStatus.ok()) {
	  // FIXME: How to handle this? How can pretty print fail? Maybe just abort?
	  throw std::runtime_error(arrowStatus.detail()->ToString());
	}

	return ss.str();
  }
  else{
    return "<empty>";
  }
}

std::shared_ptr<normal::core::TupleSet> TupleSet2::toTupleSetV1() {

  // FIXME: V1 tuple sets cant be empty

  return normal::core::TupleSet::make(table_.value());
}
