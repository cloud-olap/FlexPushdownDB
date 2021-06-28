//
// Created by matt on 12/12/19.
//

#include "normal/tuple/TupleSet.h"
#include "normal/tuple/Globals.h"

#include <utility>
#include <sstream>
#include <cassert>
#include <cstdlib>                      // for abort
#include <memory>                        // for __shared_ptr_access, __share...

#include <arrow/api.h>                 // for Array, NumericArray, StringA...
#include <arrow/csv/api.h>            // for TableReader
#include <tl/expected.hpp>
#include <arrow/scalar.h>

namespace arrow { class MemoryPool; }

using namespace normal::tuple;

std::shared_ptr<TupleSet> TupleSet::make(const std::shared_ptr<arrow::csv::TableReader> &tableReader) {

  auto result = tableReader->Read();
  if (!result.ok()) {
    throw std::runtime_error(result.status().message());
  }

  auto tupleSet = std::make_shared<TupleSet>();
  auto table = result.ValueOrDie();
  tupleSet->table_ = table;

  assert(tupleSet);
  assert(tupleSet->table_);
  assert(tupleSet->table_->ValidateFull().ok());

  return tupleSet;
}

std::shared_ptr<TupleSet> TupleSet::make(const std::shared_ptr<arrow::Table> &table) {

  auto tupleSet = std::make_shared<TupleSet>();
  tupleSet->table_ = table;

  return tupleSet;
}

std::shared_ptr<arrow::Table> TupleSet::table() const {
  return table_;
}

void TupleSet::table(const std::shared_ptr<arrow::Table> &table) {
  table_ = table;
}

std::shared_ptr<TupleSet> TupleSet::concatenate(const std::shared_ptr<TupleSet> &tp1,
                                                const std::shared_ptr<TupleSet> &tp2) {


#ifndef NDEBUG
  {
	assert(tp1);
	assert(tp2);
	assert(tp1->table_);
	assert(tp2->table_);

	auto res = tp1->table_->ValidateFull();
	if (!res.ok()) {
	  throw std::runtime_error(res.message());
	}

	res = tp2->table_->ValidateFull();
	if (!res.ok()) {
	  throw std::runtime_error(res.message());
	}

  }
#endif

  std::shared_ptr<arrow::Table> tb1 = tp1->table_;
  std::shared_ptr<arrow::Table> tb2 = tp2->table_;
  std::vector<std::shared_ptr<arrow::Table>> tblVector = {tb1, tb2};

  auto res = arrow::ConcatenateTables(tblVector);
  if (!res.ok())
    throw std::runtime_error(res.status().message());
  auto resTupleSet = make(*res);
  return resTupleSet;
}

int64_t TupleSet::numRows() {
  return table_->num_rows();
}

int64_t TupleSet::numColumns() {
  return table_->num_columns();
}

std::shared_ptr<arrow::Scalar> TupleSet::visit(const std::function<std::shared_ptr<arrow::Scalar>(std::shared_ptr<arrow::Scalar>, arrow::RecordBatch &)> &fn) {

  arrow::Status arrowStatus;

  std::shared_ptr<arrow::RecordBatch> batch;
  arrow::TableBatchReader reader(*table_);
//  reader.set_chunksize(tuple::DefaultChunkSize);
  arrowStatus = reader.ReadNext(&batch);

  std::shared_ptr<arrow::Scalar> result;
  while (arrowStatus.ok() && batch) {
    result = fn(result, *batch);
    arrowStatus = reader.ReadNext(&batch);
  }

  return result;
}

/**
 *
 * @return
 */
std::string TupleSet::toString() {

  auto ss = std::stringstream();
  arrow::Status arrowStatus = arrow::PrettyPrint(*table_, 0, &ss);

  if (!arrowStatus.ok()) {
    // FIXME
	throw std::runtime_error(arrowStatus.message());
  }

  return ss.str();
}
