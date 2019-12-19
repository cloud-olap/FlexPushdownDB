//
// Created by matt on 12/12/19.
//

#include "normal/core/TupleSet.h"

#include <utility>
#include <sstream>
#include <cassert>
#include <cstdlib>                      // for abort
#include <memory>                        // for __shared_ptr_access, __share...

#include <arrow/api.h>                 // for Array, NumericArray, StringA...
#include <arrow/csv/api.h>            // for TableReader

namespace arrow { class MemoryPool; }


std::shared_ptr<TupleSet> TupleSet::make(const std::shared_ptr<arrow::csv::TableReader>& tableReader) {

  auto result = tableReader->Read();
  if (!result.ok()) {
    // FIXME
    abort();
  }

  auto tupleSet = std::make_shared<TupleSet>();
  auto table = result.ValueOrDie();
  tupleSet->m_table = table;

  assert(tupleSet);
  assert(tupleSet->m_table);
  assert(tupleSet->m_table->ValidateFull().ok());

  return tupleSet;
}

std::shared_ptr<TupleSet> TupleSet::make(std::shared_ptr<arrow::Table> table) {

  auto tupleSet = std::make_shared<TupleSet>();
  tupleSet->m_table = std::move(table);

  return tupleSet;
}

std::shared_ptr<arrow::Table> TupleSet::getTable() const {
  return m_table;
}

void TupleSet::setTable(const std::shared_ptr<arrow::Table> &table) {
  m_table = table;
}

void TupleSet::addColumn(const std::string name, int position, std::vector<std::shared_ptr<std::string>> data) {
  arrow::Status arrowStatus;

  arrow::MemoryPool *pool = arrow::default_memory_pool();
  arrow::StringBuilder colBuilder(pool);

  for (int64_t r = 0; r < m_table->num_rows(); ++r) {
    std::shared_ptr<std::string> s = data.at(r);
    arrowStatus = colBuilder.Append(s->c_str()); // FIXME: Not sure if this is safe

    if(!arrowStatus.ok())
      abort();
  }

  std::shared_ptr<arrow::StringArray> col;
  arrowStatus = colBuilder.Finish(&col);

  if(!arrowStatus.ok())
    abort();

  auto chunked_col = std::make_shared<arrow::ChunkedArray>(col);

  std::shared_ptr<arrow::Field> field;
  field = arrow::field(name, arrow::utf8());

  arrowStatus = m_table->AddColumn(position, field, chunked_col, &m_table);

  if(!arrowStatus.ok())
    abort();
}

int TupleSet::numRows() {
  return m_table->num_rows();
}

std::string TupleSet::visit(std::string (*fn)(std::string, arrow::RecordBatch&)) {

  arrow::Status arrowStatus;

  std::shared_ptr<arrow::RecordBatch> batch;
  arrow::TableBatchReader reader(*m_table);
  reader.set_chunksize(10);
  arrowStatus = reader.ReadNext(&batch);

  std::string result;
  while(arrowStatus.ok() && batch) {
    result = fn(result, *batch);
    arrowStatus = reader.ReadNext(&batch);
  }

  return result;

//    std::shared_ptr<arrow::Array> array = batch->column(column);
//
//    std::shared_ptr<arrow::DataType> colType = array->type();
//    if(colType->Equals(arrow::Int64Type())) {
//      std::shared_ptr<arrow::Int64Array >
//          typedArray = std::static_pointer_cast<arrow::Int64Array>(array);
//      auto v = typedArray->Value(row);
//      return std::to_string(v);
//    }
//    else if(colType->Equals(arrow::StringType())){
//      std::shared_ptr<arrow::StringArray>
//          typedArray = std::static_pointer_cast<arrow::StringArray>(array);
//      auto v = typedArray->GetString(row);
//      return v;
//    }
//    else if(colType->Equals(arrow::DoubleType())){
//      std::shared_ptr<arrow::DoubleArray>
//          typedArray = std::static_pointer_cast<arrow::DoubleArray>(array);
//      auto v = typedArray->Value(row);
//      return std::to_string(v);
//    }
//    else{
//      abort();
//    }
//
//    arrowStatus = reader.ReadNext(&batch);
}

/**
 *
 * @return
 */
std::string TupleSet::toString() {

  auto ss = std::stringstream();
  arrow::Status arrowStatus = arrow::PrettyPrint(*m_table, 0, &ss);

  if(!arrowStatus.ok()) {
    // FIXME
    abort();
  }

  return ss.str();
}
