//
// Created by matt on 3/8/20.
//

#include <fpdb/tuple/TupleSetIndexFinder.h>
#include <fpdb/tuple/ColumnName.h>
#include <fmt/format.h>

#include <utility>

namespace fpdb::tuple {

TupleSetIndexFinder::TupleSetIndexFinder(shared_ptr<TupleSetIndex> tupleSetIndex,
                                         vector<int> probeColumnIndexes,
                                         shared_ptr<arrow::RecordBatch> recordBatch) :
  tupleSetIndex_(move(tupleSetIndex)),
  probeColumnIndexes_(move(probeColumnIndexes)),
  recordBatch_(move(recordBatch)) {}

tl::expected<shared_ptr<TupleSetIndexFinder>, string>
TupleSetIndexFinder::make(const shared_ptr<TupleSetIndex> &tupleSetIndex,
                          const vector<string> &probeColumnNames,
                          const shared_ptr<arrow::RecordBatch> &recordBatch) {
  // Get the column indexes, checking the column exists
  vector<int> probeColumnIndexes;
  for (const auto &columnName: probeColumnNames) {
    auto columnIndex = recordBatch->schema()->GetFieldIndex(ColumnName::canonicalize(columnName));
    if (columnIndex == -1) {
      return tl::make_unexpected(fmt::format("Cannot make TupleSetIndexFinder. Column '{}' does not exist", columnName));
    }
    probeColumnIndexes.emplace_back(columnIndex);
  }

  return make_shared<TupleSetIndexFinder>(tupleSetIndex, probeColumnIndexes, recordBatch);
}

tl::expected<vector<int64_t>, string> TupleSetIndexFinder::find(int64_t rowIndex) {
  // Make tupleKey
  const auto &expTupleKey = TupleKeyBuilder::make(rowIndex, probeColumnIndexes_, *recordBatch_);
  if (!expTupleKey.has_value()) {
    return tl::make_unexpected(expTupleKey.error());
  }

  // Find
  const auto &tupleKey = expTupleKey.value();
  return tupleSetIndex_->find(tupleKey);
}

}
